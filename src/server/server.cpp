#include "server.h"
#include <grpcpp/alarm.h>

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
    SPDLOG_LOGGER_INFO(logger, "Received write request");
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

    while (true) {
        grpc::CompletionQueue broadcast_queue;
        std::vector<InvalidateResponse> responses;
        std::vector<grpc::Status> broadcast_status_list;
        {
            std::unique_lock<std::mutex> server_state_lock {server_state_mutex};
            broadcast_invalidate(hermes_val, key, broadcast_queue, responses, broadcast_status_list);
        }

        // Check if the write was interrupted by a higher priority write
        if (!hermes_val->is_valid()) {
            SPDLOG_LOGGER_INFO(logger, "Received Invalidate RPC in the middle of write RPC");
            // TODO(): Skip transient state and go to invalid state directly?
            broadcast_queue.Shutdown();
            // Value is in transient state waiting for ACKs to arrive
            hermes_val->coord_write_to_trans_transition();
            hermes_val->wait_for_acks();
            hermes_val->coord_trans_to_invalid_transition();

            // start timer..
            // From here we need to wait till the other coordinator sends the updated value
            // with the correct timestamp. We wait till the state becomes valid since we are 
            // currently in an INVALID state. Once done, we can return the value and the
            // client sees the latest updated value based on the timestamp ordering
            hermes_val->wait_till_valid();
            // timer expires first or we get a validate rpc
            // if timer expires first:
            //      go to replay state 

        } else {
            // Wait till all the acks for the invalidate arrives 
            int acks_received = receive_acks(broadcast_queue, responses, broadcast_status_list);
        
            if (acks_received == responses.size()) {
                // Value was accepted by all the nodes, we can trasition safely back to valid state
                // and propagate a VAL message to all the nodes. Wait till we get ACKs back (do we need this??)
                hermes_val->coord_write_to_valid_transition();
                hermes_val->wait_for_acks();
                break;
            }
        }
    }
    return grpc::Status::OK;
}

int HermesServiceImpl::receive_acks(grpc::CompletionQueue &cq, std::vector<InvalidateResponse> &responses, 
        std::vector<grpc::Status> &status_list) {
    int acks_received = 0;
    int num_servers, alarm_tag;
    num_servers = alarm_tag = responses.size();
    void* next_tag;
    bool ok;
    bool retry = false;
    bool key_invalidated = false;
    // TODO(): Check state
    while (!cq.Next(&next_tag, &ok)) {
        // TODO(): Check state
        if (ok) {
            if (*reinterpret_cast<int*>(next_tag) == alarm_tag) {
                // MLT expired. Return from this function and keep retrying...
                SPDLOG_LOGGER_INFO(logger, "Alarm expired while broadcasting");
                break;
            } else {
                acks_received++;
                if (acks_received == num_servers) {
                    break;
                }
            }
        }
    }
    cq.Shutdown();
    SPDLOG_LOGGER_INFO(logger, "Received %d acks", acks_received);
    return acks_received;
}

void HermesServiceImpl::broadcast_invalidate(HermesValue *val, std::string &key, grpc::CompletionQueue &cq,
        std::vector<InvalidateResponse> &responses, std::vector<grpc::Status> &status_list) {   
    SPDLOG_LOGGER_INFO(logger, "Broadcasting INVALIDATE RPCs");
    int num_active_servers = active_server_stubs.size();
    responses.resize(num_active_servers);
    status_list.resize(num_active_servers);
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(mlt);

    grpc::Alarm alarm;
    alarm.Set(&cq, deadline, reinterpret_cast<void*>(&num_active_servers));

    int i = 0;
    auto grpc_ts = val->timestamp.get_grpc_timestamp();

    for (auto& stub: active_server_stubs) {
        // Send invalidates
        grpc::ClientContext ctx;
        InvalidateRequest req;
        req.set_allocated_key(&key);
        req.set_allocated_ts(&grpc_ts);
        req.set_value(val->value);
        req.set_epoch_id(epoch);

        InvalidateResponse resp;
        auto receiver = stub->AsyncInvalidate(&ctx, req, &cq);
        receiver->Finish(&responses[i], &status_list[i], (void*)i);
        i++;
    }
    SPDLOG_LOGGER_DEBUG(logger, "Broadcasted Invalidate RPCs");
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
