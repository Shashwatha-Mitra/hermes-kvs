#include <csignal>
#include <cstdlib>

#include "server.h"
#include <grpcpp/alarm.h>

using map_iterator = typename std::unordered_map<std::string, std::unique_ptr<HermesValue>>::iterator;

template<typename ResponseType>
struct GrpcAsyncCall {
    int tag_value;
    grpc::Status status;
    grpc::ClientContext ctx;
    ResponseType response;

    GrpcAsyncCall(int i): tag_value(i) {};
};

std::unique_ptr<Hermes::Stub> create_stub(const std::string &addr) {
    auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return std::make_unique<Hermes::Stub>(channel_ptr);
}

HermesServiceImpl::HermesServiceImpl(uint32_t id, std::string &log_dir, 
        const std::vector<std::string> &server_list,
        uint32_t port,
        std::atomic<bool>& terminate_flag)
        : server_id(id), epoch(0) {
    active_servers = std::move(server_list);
    self_addr = "localhost:" + std::to_string(port);

    for (auto server: active_servers) {
        if (server == self_addr) continue;
        active_server_stubs.push_back(create_stub(server));
    }

    // Logger initialization
    std::string log_file_name = log_dir + "spdlog_server_" + std::to_string(id) + ".log";

    // Initialize the logger and set the flush rate
    //spdlog::flush_every(std::chrono::microseconds(100));
    spdlog::flush_every(std::chrono::milliseconds(1));
    logger = spdlog::basic_logger_mt("server_logger", log_file_name);

    // Set logging level
    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::debug);
}

//HermesServiceImpl::~HermesServiceImpl() {
//    SPDLOG_LOGGER_INFO(logger , "killing node");
//    spdlog::drop("server_logger");
//    if (spdlog::get("server_logger") == nullptr) {
//        std::cout << "logger has been successfully deregistered\n";
//    }
//    terminate();
//}

HermesServiceImpl::~HermesServiceImpl() {}

grpc::Status HermesServiceImpl::Read(grpc::ServerContext *ctx, 
        const ReadRequest *req, ReadResponse *resp) {
    std::string key = req->key();

    SPDLOG_LOGGER_INFO(logger, "Received Read Request!");
    SPDLOG_LOGGER_DEBUG(logger, "key: {}", key);

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
    std::string key = req->key();
    std::string value = req->value();

    SPDLOG_LOGGER_INFO(logger, "Received write request");
    SPDLOG_LOGGER_DEBUG(logger, "key: {}", key);
    SPDLOG_LOGGER_DEBUG(logger, "value: {}", value);

    HermesValue *hermes_val;
    map_iterator it;
    map_iterator end_it;
    {
        std::shared_lock<std::shared_mutex> lock {hashmap_mutex};
        it = key_value_map.find(key);
        end_it = key_value_map.end();
    }
    if (it == end_it) {
        SPDLOG_LOGGER_DEBUG (logger, "Key not found!");
        {
            std::unique_lock<std::shared_mutex> lock {hashmap_mutex};
            key_value_map[key] = std::make_unique<HermesValue>(value, server_id);
            hermes_val = key_value_map[key].get();
        }
    } else {
        SPDLOG_LOGGER_DEBUG (logger, "Key found!");
        hermes_val = it->second.get();
    }

    // Stall writes till we are sure that the key is valid
    hermes_val->wait_till_valid();
    /**
    TODO: Debug this!!
    Compare and Swap operations usually should succeed. But since we are using enums as is
    we could face failures. If we are continually succeeding, we can drop this check and 
    make coord_valid_to_write_transition() inline void to make this cleaner. 
    Save the write timestamp in a local variable. Otherwise it might get overwritten during 
    invalidate and wrong ts might be propragated in an invalidate RPC
    */
    auto write_ts = hermes_val->coord_valid_to_write_transition(value, server_id);
    // } else {
    //     SPDLOG_LOGGER_CRITICAL(logger, "Compare and Swap failed!!. Value still in VALID state.");
    // }

    while (true) {
        grpc::CompletionQueue broadcast_queue;
        {
            std::unique_lock<std::mutex> server_state_lock {server_state_mutex};
            broadcast_invalidate(write_ts, value, key, broadcast_queue);
        }

        // Check if the write was interrupted by a higher priority write
        if (!hermes_val->is_write()) {
            // TODO(): This shouldn't be required. Just return
            SPDLOG_LOGGER_INFO(logger, "Received Invalidate RPC in the middle of write RPC. Aborting write.");
            broadcast_queue.Shutdown();
            break;
        }
        
        // Wait till all the acks for the invalidate arrives 
        auto res = receive_acks(broadcast_queue);
        int acks = res.first;
        int acceptances = res.second;
    
        if (acceptances == active_server_stubs.size()) {
            SPDLOG_LOGGER_DEBUG(logger, "Received all acceptances");
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
    return grpc::Status::OK;
}

std::pair<int, int> HermesServiceImpl::receive_acks(grpc::CompletionQueue &cq) {
    int acks_received = 0;
    int acceptances_received = 0;
    int num_servers, alarm_tag;
    // Don't use the set of servers since it might be modified by a parallel thread and we don't want locks here
    num_servers = active_server_stubs.size();
    alarm_tag = -1;
    void* next_tag;
    bool ok;

    while (cq.Next(&next_tag, &ok)) {
        if (ok) {
            GrpcAsyncCall<InvalidateResponse>* grpc_tag = static_cast<GrpcAsyncCall<InvalidateResponse>*>(next_tag);
            if (grpc_tag->tag_value == alarm_tag) {
                // MLT expired. Return from this function and keep retrying...
                SPDLOG_LOGGER_INFO(logger, "Alarm expired while broadcasting");
                break;
            } else {
                acks_received++;
                if (grpc_tag->response.accept()) {
                    acceptances_received++;
                }
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
    SPDLOG_LOGGER_INFO(logger, "Received " + std::to_string(acceptances_received) + " acceptances");
    return std::make_pair<>(acks_received, acceptances_received);
}

void HermesServiceImpl::broadcast_invalidate(Timestamp &ts, const std::string &value, std::string &key, 
        grpc::CompletionQueue &cq) {   
    SPDLOG_LOGGER_INFO(logger, "Broadcasting INVALIDATE RPCs");
    int num_other_servers = active_server_stubs.size();
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(mlt);

    grpc::Alarm alarm;
    int alarm_tag = -1;
    alarm.Set(&cq, deadline, reinterpret_cast<void*>(&alarm_tag));

    uint64_t i = 0;
    HermesTimestamp grpc_ts = ts.get_grpc_timestamp();

    for (auto& stub: active_server_stubs) {
        // Send invalidates
        InvalidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        req.set_value(value);
        req.set_epoch_id(epoch);
        GrpcAsyncCall<InvalidateResponse>* call = new GrpcAsyncCall<InvalidateResponse>(i);

        auto receiver = stub->AsyncInvalidate(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "Broadcasted Invalidate RPCs");
}

void HermesServiceImpl::broadcast_validate(Timestamp ts, std::string key) {
    grpc::CompletionQueue cq;
    SPDLOG_LOGGER_INFO(logger, "Broadcasting VALIDATE RPCs");
    int num_other_servers = active_server_stubs.size();

    uint64_t i = 0;
    HermesTimestamp grpc_ts = ts.get_grpc_timestamp();

    for (auto& stub: active_server_stubs) {
        ValidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        GrpcAsyncCall<Empty>* call = new GrpcAsyncCall<Empty>(i);

        auto receiver = stub->AsyncValidate(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "Broadcasted validate RPCs");
    // Dont wait for responses
    cq.Shutdown();
}

// Invalidate handling via gRPC
grpc::Status HermesServiceImpl::Invalidate(grpc::ServerContext *ctx, const InvalidateRequest *req, InvalidateResponse *resp) {
    HermesTimestamp ts = req->ts();
    SPDLOG_LOGGER_INFO(logger, "Received Invalidate RPC from node_id: {}", Timestamp(ts).node_id);
    if (req->epoch_id() != epoch) {
        // Epoch id doesnt match. Reject request
        SPDLOG_LOGGER_DEBUG(logger, "Rejecting invalidate request because received epoch_id {} doesn't match with local epoch id {}", req->epoch_id(), epoch);
        resp->set_accept(false);
        return grpc::Status::OK;
    }
    auto value = req->value();
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
        SPDLOG_LOGGER_DEBUG(logger, "Rejecting invalidate request because received timestamp {} is lower than local timestamp {}", Timestamp(ts).toString(), hermes_val->timestamp.toString());
        return grpc::Status::OK;
    }
    hermes_val->fol_invalidate(value, ts);
    SPDLOG_LOGGER_INFO(logger, "Accepting Invalidate RPC");
    resp->set_accept(true);

    // TODO(): Move this code to a different thread
    if (false) {
        // From here we need to wait till the other coordinator sends the updated value
        // with the correct timestamp. We wait till the state becomes valid since we are 
        // currently in an INVALID state. Once done, we can return the value and the
        // client sees the latest updated value based on the timestamp ordering
        hermes_val->wait_till_valid_or_timeout(replay_timeout);
        
        if (!hermes_val->is_valid()) {
            // We did not receive a VAL message from the conflicting write within the timeout
            // Since we are a follower now, we make the follower INVALID to REPLAY transition
            // TODO(): Make a separate function for replay
            hermes_val->fol_invalid_to_replay_transition();
        }
    }
    return grpc::Status::OK;
}

// Called by co-ordinator to validate the current key.
grpc::Status HermesServiceImpl::Validate(grpc::ServerContext *ctx, const ValidateRequest *req, Empty *resp) {
    auto& ts = req->ts();
    SPDLOG_LOGGER_INFO(logger, "Received validate RPC from node_id: {}", Timestamp(ts).node_id);
    auto& key = req->key();
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

// Called the server which is going down
grpc::Status HermesServiceImpl::Mayday(grpc::ServerContext *ctx, const MaydayRequest *req, Empty *resp) {
    SPDLOG_LOGGER_CRITICAL(logger, "node_id {} is failing gracefully", req->node_id());
    return grpc::Status::OK;
}

void HermesServiceImpl::broadcast_mayday(grpc::CompletionQueue &cq) {
    SPDLOG_LOGGER_INFO(logger, "Broadcasting Mayday RPCs");
    int num_other_servers = active_server_stubs.size();

    uint64_t i = 0;

    for (auto& stub: active_server_stubs) {
        MaydayRequest req;
        req.set_node_id(server_id);
        GrpcAsyncCall<Empty>* call = new GrpcAsyncCall<Empty>(i);

        auto receiver = stub->AsyncMayday(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "Broadcasted Mayday RPCs");
}

void HermesServiceImpl::receive_mayday_acks(grpc::CompletionQueue &cq) {
    int acks_received = 0;
    int num_servers;
    // Don't use the set of servers since it might be modified by a parallel thread and we don't want locks here
    num_servers = active_server_stubs.size();
    void* next_tag;
    bool ok;

    while (cq.Next(&next_tag, &ok)) {
        if (ok) {
            GrpcAsyncCall<Empty>* grpc_tag = static_cast<GrpcAsyncCall<Empty>*>(next_tag);
            acks_received++;
            if (acks_received == num_servers) {
                break;
            }
        } else {
            SPDLOG_LOGGER_CRITICAL(logger, "Not okay!");
        }
        // delete grpc_tag;
    }
    cq.Shutdown();
    SPDLOG_LOGGER_INFO(logger, "Received " + std::to_string(acks_received) + " acks");
}

grpc::Status HermesServiceImpl::Terminate(grpc::ServerContext *ctx, const TerminateRequest *req, Empty *resp) {
    SPDLOG_LOGGER_CRITICAL(logger, "Terminating because client called Terminate RPC");
    terminate(req->graceful());
    return grpc::Status::OK;
}

void HermesServiceImpl::terminate(bool graceful) {
    if (graceful) {
        SPDLOG_LOGGER_CRITICAL(logger, "terminating gracefully");
        grpc::CompletionQueue mayday_queue;
        broadcast_mayday(mayday_queue);
        receive_mayday_acks(mayday_queue);
    }
    else {
        SPDLOG_LOGGER_CRITICAL(logger, "not implemented");
    }
}