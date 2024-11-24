#include "server.h"
#include <grpcpp/alarm.h>

using map_iterator = typename std::unordered_map<std::string, std::unique_ptr<HermesValue>>::iterator;

struct GrpcAsyncCall {
    int tag_value;
    grpc::Status status;
    grpc::ClientContext ctx;

    GrpcAsyncCall(int i): tag_value(i) {};
};

std::unique_ptr<Hermes::Stub> create_stub(const std::string &addr) {
    auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return std::make_unique<Hermes::Stub>(channel_ptr);
}

HermesServiceImpl::HermesServiceImpl(uint32_t id, std::string &log_dir, 
        const std::vector<std::string> &server_list,
        uint32_t port)
        : server_id(id) {
    active_servers = std::move(server_list);
    self_addr = "localhost:" + std::to_string(port);

    for (auto server: active_servers) {
        if (server == self_addr) continue;
        active_server_stubs.push_back(create_stub(server));
    }
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
        const ReadRequest *req, ReadResponse *resp) {
    auto key = req->key();
    SPDLOG_LOGGER_INFO(logger, "Received Read Request!");
    map_iterator it;
    map_iterator end_it;
    {
        std::shared_lock<std::shared_mutex> lock {hashmap_mutex};
        it = key_value_map.find(key);
        end_it = key_value_map.end();
    }
    if (it == end_it) {
        std::string not_found = "Key not found";
        resp->set_value(not_found);
        return grpc::Status::OK;
    }

    const auto hermes_val = it->second.get();
    hermes_val->wait_till_valid();
    resp->set_value(hermes_val->value);
    return grpc::Status::OK;
}

grpc::Status HermesServiceImpl::Write(grpc::ServerContext *ctx, const WriteRequest *req, Empty *resp) {
    auto key = req->key();
    SPDLOG_LOGGER_INFO(logger, "Received write request");
    HermesValue *hermes_val;
    // TODO(): Add locks
    if (key_value_map.find(key) == key_value_map.end()) {
        SPDLOG_LOGGER_DEBUG (logger, "Key not found!");
        key_value_map[key] = std::make_unique<HermesValue>(req->value(), server_id);
        hermes_val = key_value_map[key].get();
    } else {
        SPDLOG_LOGGER_DEBUG (logger, "Key found!");
        hermes_val = key_value_map.find(key)->second.get();
    }

    // Stall writes till we are sure that the key is valid
    hermes_val->wait_till_valid();
    bool replay = false;

    // TODO: Debug this!!
    // Compare and Swap operations usually should succeed. But since we are using enums as is
    // we could face failures. If we are continually succeeding, we can drop this check and 
    // make coord_valid_to_write_transition() inline void to make this cleaner. 
    if (hermes_val->coord_valid_to_write_transition()) {
        SPDLOG_LOGGER_INFO(logger, "Compare and Swap succeeded. Value in WRITE state");
        hermes_val->increment_ts(server_id);
        hermes_val->update_value(req->value());
    } else {
        SPDLOG_LOGGER_CRITICAL(logger, "Compare and Swap failed!!. Value still in VALID state.");
    }

    while (true) {
        grpc::CompletionQueue broadcast_queue;
        std::vector<InvalidateResponse> responses;
        {
            std::unique_lock<std::mutex> server_state_lock {server_state_mutex};
            broadcast_invalidate(hermes_val, key, broadcast_queue, responses);
        }

        // Check if the write was interrupted by a higher priority write
        if (!hermes_val->is_write()) {
            SPDLOG_LOGGER_INFO(logger, "Received Invalidate RPC in the middle of write RPC");
            broadcast_queue.Shutdown();

            // Value is in invalid state. We do not wait for ACKs for the previous INV RPC.
            hermes_val->coord_write_to_invalid_transition();

            // From here we need to wait till the other coordinator sends the updated value
            // with the correct timestamp. We wait till the state becomes valid since we are 
            // currently in an INVALID state. Once done, we can return the value and the
            // client sees the latest updated value based on the timestamp ordering
            hermes_val->wait_till_valid_timeout(replay_timeout);
            
            if (!hermes_val->is_valid()) {
                // We did not receive a VAL message from the conflicting write within the timeout
                // Since we are a follower now, we make the follower INVALID to REPLAY transition
                // TODO(): Make a separate function for replay
                hermes_val->fol_invalid_to_replay_transition();
                replay = true;
            } else {
                // We received a VAL message from the conflicting write. We can safely return.
                // Insert key into the KV-store
                hermes_val->increment_ts(server_id);
                break;
            }
        } else {
            // Wait till all the acks for the invalidate arrives 
            int acks_received = receive_acks(broadcast_queue, responses);
        
            if (acks_received == responses.size()) {
                SPDLOG_LOGGER_DEBUG(logger, "Received all acks");
                // Value was accepted by all the nodes, we can trasition safely back to valid state
                // and propagate a VAL message to all the nodes. Wait till we get ACKs back (do we need this??)
                // auto thread = std::thread(std::bind(&HermesServiceImpl::broadcast_validate, this, hermes_val->timestamp, key));
                {
                    std::unique_lock<std::mutex> server_state_lock {server_state_mutex};
                    broadcast_validate(hermes_val->timestamp, key);
                }
                hermes_val->coord_write_to_valid_transition();
                break;
            }
        }
    }
    return grpc::Status::OK;
}

int HermesServiceImpl::receive_acks(grpc::CompletionQueue &cq, std::vector<InvalidateResponse> &responses) {
    int acks_received = 0;
    int num_servers, alarm_tag;
    // Don't use the set of servers since it might be modified by a parallel thread and we don't want locks here
    num_servers = alarm_tag = responses.size();
    void* next_tag;
    bool ok;

    // TODO(): Check state
    while (cq.Next(&next_tag, &ok)) {        
        if (ok) {
            GrpcAsyncCall* grpc_tag = static_cast<GrpcAsyncCall*>(next_tag);
            if (grpc_tag->tag_value == alarm_tag) {
                // MLT expired. Return from this function and keep retrying...
                SPDLOG_LOGGER_INFO(logger, "Alarm expired while broadcasting");
                break;
            } else {
                acks_received++;
                if (acks_received == num_servers) {
                    break;
                }
            }
        } else {
            SPDLOG_LOGGER_CRITICAL(logger, "Not okay!");
        }
        // delete grpc_tag;
    }
    cq.Shutdown();
    SPDLOG_LOGGER_INFO(logger, "Received " + std::to_string(acks_received) + " acks");
    return acks_received;
}

void HermesServiceImpl::broadcast_invalidate(HermesValue *val, std::string &key, grpc::CompletionQueue &cq,
        std::vector<InvalidateResponse> &responses) {   
    SPDLOG_LOGGER_INFO(logger, "Broadcasting INVALIDATE RPCs");
    int num_other_servers = active_server_stubs.size();
    responses.resize(num_other_servers);   
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(mlt);

    grpc::Alarm alarm;
    alarm.Set(&cq, deadline, reinterpret_cast<void*>(&num_other_servers));

    uint64_t i = 0;
    auto grpc_ts = val->timestamp.get_grpc_timestamp();

    for (auto& stub: active_server_stubs) {
        // Send invalidates
        InvalidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        req.set_value(val->value);
        req.set_epoch_id(epoch);
        GrpcAsyncCall* call = new GrpcAsyncCall(i);

        auto receiver = stub->AsyncInvalidate(&call->ctx, req, &cq);
        receiver->Finish(&responses[i], &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "Broadcasted Invalidate RPCs");
}

void HermesServiceImpl::broadcast_validate(Timestamp ts, std::string key) {
    grpc::CompletionQueue cq;
    std::vector<Empty> responses;

    SPDLOG_LOGGER_INFO(logger, "Broadcasting VALIDATE RPCs");
    int num_other_servers = active_server_stubs.size();
    responses.resize(num_other_servers);

    uint64_t i = 0;
    auto grpc_ts = ts.get_grpc_timestamp();

    for (auto& stub: active_server_stubs) {
        ValidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        GrpcAsyncCall* call = new GrpcAsyncCall(i);

        auto receiver = stub->AsyncValidate(&call->ctx, req, &cq);
        receiver->Finish(&responses[i], &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "Broadcasted validate RPCs");
    // Dont wait for responses
    cq.Shutdown();
}

// Invalidate handling via gRPC
grpc::Status HermesServiceImpl::Invalidate(grpc::ServerContext *ctx, const InvalidateRequest *req, InvalidateResponse *resp) {
    SPDLOG_LOGGER_INFO(logger, "Received Invalidate RPC");
    if (req->epoch_id() != epoch) {
        // Epoch id doesnt match. Reject request
        resp->set_accept(false);
        return grpc::Status::OK;
    }
    auto value = req->value();
    auto ts = req->ts();
    HermesValue* hermes_val {nullptr};
    bool new_key = false;
    
    if (key_value_map.find(req->key()) == key_value_map.end()) {
        // Key not found. This corresponds to an insertion 
        key_value_map[req->key()] = std::make_unique<HermesValue>(req->value(), server_id);
        new_key = true;
    }
    
    hermes_val = key_value_map.find(req->key())->second.get();

    // Reject any key that has lower timestamp
    if (!new_key && hermes_val->is_lower(ts)) {
        // Timestamp is lower than local timestamp. Reject
        resp->set_accept(false);
        SPDLOG_LOGGER_INFO(logger, "Rejecting Invalidate RPC");
        return grpc::Status::OK;
    }
    hermes_val->fol_valid_to_invalid_transition(value, ts);
    
    {
        std::unique_lock<std::mutex> lock {hermes_val->stall_mutex};
        hermes_val->timestamp = Timestamp(req->ts());
        hermes_val->value = req->value();
    }

    SPDLOG_LOGGER_INFO(logger, "Accepting Invalidate RPC");
    resp->set_accept(true);

    // Go to replay state after timeout
    return grpc::Status::OK;
}

// Called by co-ordinator to validate the current key.
grpc::Status HermesServiceImpl::Validate(grpc::ServerContext *ctx, const ValidateRequest *req, Empty *resp) {
    SPDLOG_LOGGER_INFO(logger, "Received validate RPC");
    auto& key = req->key();
    auto& ts = req->ts();
    HermesValue* hermes_val = key_value_map.find(req->key())->second.get();
    // SPDLOG_LOGGER_DEBUG(logger, )
    if (hermes_val->not_equal(ts)) {
        // Timestamp is not equal to local timestamp, which means a request with higher timestamp must 
        // have been accepted. Ignore
        return grpc::Status::OK;
    }
    hermes_val->fol_invalid_to_valid_transition();
    SPDLOG_LOGGER_DEBUG(logger, "Validated key after write");
    return grpc::Status::OK;
}
