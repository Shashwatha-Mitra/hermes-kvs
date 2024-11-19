#include "server.h"
#include <alarm.h>

std::unique_ptr<Hermes::Stub> create_stub(const std::string &addr) {
    auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return std::make_unique<Hermes::Stub>(channel_ptr);
}

HermesServiceImpl::HermesServiceImpl(uint32_t id, std::string &log_dir): server_id(id) {
    std::string log_file_name = log_dir + "spdlog_server_" + std::to_string(id) + ".log";

    // Initialize the logger and set the flush rate
    spdlog::flush_every(std::chrono::milliseconds(1));
    logger = spdlog::basic_logger_mt("server_logger", log_file_name);

    // Set logging level
    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::debug);
    SPDLOG_LOGGER_TRACE(logger , "Trace level logging.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_DEBUG(logger , "Debug level logging.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_INFO(logger , "Info level logging.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_WARN(logger , "Warn level logging.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_ERROR(logger , "Error level logging.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_CRITICAL(logger , "Critical level logging.. {} ,{}", 1, 3.23);
}

grpc::Status HermesServiceImpl::Read(grpc::ServerContext *ctx, 
        ReadRequest *req, ReadResponse *resp) {
    auto key = req->key();
    if (key_value_map.find(key) == key_value_map.end()) {
        return grpc::Status::OK;
    }

    auto it = key_value_map.find(key);
    const auto hermes_val = it->second.get();
    hermes_val->wait_till_valid();
    resp->set_value(hermes_val->value);
    return grpc::Status::OK;
}

grpc::Status HermesServiceImpl::Write(grpc::ServerContext *ctx, WriteRequest *req, Empty *resp) {
    auto key = req->key();
    HermesValue *hermes_val;
    if (key_value_map.find(key) == key_value_map.end()) {
        hermes_val = new HermesValue(req->value(), server_id);
    } else {
        hermes_val = key_value_map.find(key)->second.get();
    }
    
    // Stall writes till we are sure that the key is valid
    hermes_val->wait_till_valid();

    // If key is available, transition to WRITE state
    hermes_val->coord_valid_to_write_transition();

    // Check if the write was interrupted by a higher priority write
    if (!hermes_val->is_valid()) {
        
        // Value is in transient state waiting for ACKs to arrive
        hermes_val->coord_write_to_trans_trasition();
        hermes_val->wait_for_acks();
        hermes_val->coord_trans_to_invalid_transition();

        // From here we need to wait till the other coordinator sends the updated value
        // with the correct timestamp. We wait till the state becomes valid since we are 
        // currently in an INVALID state. Once done, we can return the value and the
        // client sees the latest updated value based on the timestamp ordering
        hermes_val->wait_till_valid();

    } else {

        // Wait till all the acks for the invalidate arrives 
        hermes_val->wait_for_acks();
    
        // Value was accepted by all the nodes, we can trasition safely back to valid state
        // and propagate a VAL message to all the nodes. Wait till we get ACKs back (do we need this??)
        hermes_val->coord_write_to_valid_transition();
        hermes_val->wait_for_acks();

    }
    return grpc::Status::OK;
}

// Invalidates the key in all other servers by sending INV (key, val, ts, epoch)
void HermesServiceImpl::invalidate_value(HermesValue *val, std::string &key) {
    using InvalidateRespReader = typename std::unique_ptr<grpc::ClientAsyncResponseReader<InvalidateResponse>>;
    
    grpc::CompletionQueue cq;
    int num_active_servers = active_server_stubs.size();
    std::vector<InvalidateRespReader> response_receivers;
    std::vector<InvalidateResponse> responses (num_active_servers);
    std::vector<grpc::Status> status(num_active_servers); 
    std::vector<uint32_t> tags(num_active_servers);
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(mlt);
    grpc::Alarm alarm;
    void *alarm_tag = reinterpret_cast<void*>(num_active_servers);
    alarm.Set(&cq, deadline, alarm_tag);
    int i = 0;

    for (auto& stub: active_server_stubs) {
        tags[i] = i;

        // Send invalidates
        grpc::ClientContext ctx;
        InvalidateRequest req;
        req.set_allocated_key(&key);
        // TODO: Fix this memory leak
        req.set_allocated_ts(&(val->timestamp.get_grpc_timestamp()));
        req.set_value(val->value);
        req.set_epoch_id(epoch);

        InvalidateResponse resp;
        auto receiver = stub->AsyncInvalidate(&ctx, req, &cq);
        receiver->Finish(&responses[i], &status[i], (void*)i);
    }
    // Wait for response and retry
    void* next_tag;
    bool ok;
    std::vector<int> acked_servers;
    bool retry = false;
    bool key_invalidated = false;
    // TODO(): Check state
    while (!cq.Next(&next_tag, &ok)) {
        // TODO(): Check state
        if (ok) {
            if (next_tag == alarm_tag) {
                // MLT expired. Keep retrying...
                retry = true;
            } else {
                acked_servers.push_back(*reinterpret_cast<int*>(next_tag));
            }
        }
    }
    cq.Shutdown();
    alarm.Cancel();
    // Call stub->validate
}

// Invalidate handling via gRPC
grpc::Status HermesServiceImpl::Invalidate(grpc::ServerContext *ctx, InvalidateRequest *req, InvalidateResponse *resp) {
    if (req->epoch_id() != epoch) {
        // Epoch id doesnt match. Reject request
        resp->set_accept(false);
        return grpc::Status::OK;
    }
    HermesValue* hermes_val {nullptr};
    bool new_key = false;
    
    if (key_value_map.find(req->key()) != key_value_map.end()) {
        // Key not found. This corresponds to an insertion
        hermes_val = new HermesValue(req->value(), server_id);
        new_key = true;
    } else {
        hermes_val = key_value_map.find(req->key())->second.get();
    }
    {
        // State transition to Invalid
        std::unique_lock<std::mutex> lock(hermes_val->stall_mutex);
        if (!new_key && Timestamp(req->ts()) < hermes_val->timestamp) {
            // Timestamp is lower than local timestamp. Reject
            resp->set_accept(false);
            return grpc::Status::OK;
        }
        hermes_val->st = INVALID;
        hermes_val->value = req->value();
        hermes_val->timestamp = Timestamp(req->ts());
    }
    resp->set_accept(true);
    return grpc::Status::OK;
}

// Called by co-ordinator to validate the current key.
grpc::Status HermesServiceImpl::Validate(grpc::ServerContext *ctx, ValidateRequest *req, Empty *resp) {
    HermesValue* hermes_val = key_value_map.find(req->key())->second.get();
    {
        // State transition to Valid
        std::unique_lock<std::mutex> lock(hermes_val->stall_mutex);
        if (Timestamp(req->ts()) != hermes_val->timestamp) {
            // Timestamp is not equal to local timestamp, which means a request with higher timestamp must 
            // have been accepted. Ignore
            return grpc::Status::OK;
        }
        hermes_val->st = VALID;
    }
    return grpc::Status::OK;
}
