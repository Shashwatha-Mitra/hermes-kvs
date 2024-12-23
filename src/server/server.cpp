#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <chrono>

#include "server.h"
#include <grpcpp/alarm.h>

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
    // Logger initialization
    std::string log_file_name = log_dir + "/spdlog_server_" + std::to_string(id) + ".log";

    // Initialize the logger and set the flush rate
    //spdlog::flush_every(std::chrono::microseconds(100));
    spdlog::flush_every(std::chrono::milliseconds(1));
    logger = spdlog::basic_logger_mt("server_logger", log_file_name);

    // Set logging level
    logger->set_level(spdlog::level::info);
    logger->flush_on(spdlog::level::info);
    
    //active_servers = std::move(server_list);
    self_addr = "localhost:" + std::to_string(port);

    for (auto server: server_list) {
        if (server == self_addr) continue;
        //_stubs.push_back(create_stub(server));
        uint32_t other_id = addrToID(server);
        SPDLOG_LOGGER_INFO(logger, "Adding {} to active list", other_id);
        _active_servers.push_back(other_id);
        _stubs[other_id] = create_stub(server);
        //_stubs.insert(create_stub(server));
    }

    dead.store(false);
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

inline uint32_t HermesServiceImpl::portToID(uint32_t port) {
    return port;
}

uint32_t HermesServiceImpl::addrToID(std::string& addr) {
    // Find the position of the colon
    size_t colon_pos = addr.find(':');

    if (colon_pos == std::string::npos) {
        throw std::invalid_argument("Invalid address format. No colon found");
    }

    std::string port_str = addr.substr(colon_pos + 1);

    try {
        uint32_t port = std::stoul(port_str);
        return portToID(port);
    } catch (const std::invalid_argument&) {
        throw std::invalid_argument("Invalid port number format");
    } catch (const std::out_of_range&) {
        throw std::out_of_range("Port number out of range of uint32_t");
    }
}

std::string HermesServiceImpl::get_tid() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

std::pair<bool, map_iterator> HermesServiceImpl::isKeyPresent(std::string key) {
    map_iterator it;
    map_iterator end_it;
    {
        std::shared_lock<std::shared_mutex> lock {hashmap_mutex};
        it = key_value_map.find(key);
        end_it = key_value_map.end();
    }
    
    return std::make_pair(it != end_it, it);

}

map_iterator HermesServiceImpl::getValueFromDB(std::string key) {
    map_iterator it;
    map_iterator end_it;
    {
        std::shared_lock<std::shared_mutex> lock {hashmap_mutex};
        it = key_value_map.find(key);
        end_it = key_value_map.end();
    }
    assert(it != end_it);
    return it;
}

HermesValue* HermesServiceImpl::writeNewKey(std::string key, std::string value) {
    std::unique_lock<std::shared_mutex> lock {hashmap_mutex};
    key_value_map[key] = std::make_unique<HermesValue>(key, value, server_id);
    return key_value_map[key].get();
}

void HermesServiceImpl::performWrite(HermesValue *hermes_val) {
    SPDLOG_LOGGER_TRACE (logger, "[{}]::performing write", get_tid());
    uint32_t current_epoch;
    /**
    TODO: Debug this!!
    Compare and Swap operations usually should succeed. But since we are using enums as is
    we could face failures. If we are continually succeeding, we can drop this check and 
    make coord_valid_to_write_transition() inline void to make this cleaner. 
    Save the write timestamp in a local variable. Otherwise it might get overwritten during 
    invalidate and wrong ts might be propragated in an invalidate RPC
    */
    auto write_ts = hermes_val->getTimestamp(); //coord_valid_to_write_transition(value, server_id);
    // } else {
    //     SPDLOG_LOGGER_CRITICAL(logger, "Compare and Swap failed!!. Value still in VALID state.");
    // }

    std::string key = hermes_val->key;
    std::string value = hermes_val->value;

    while (true) {
        std::vector<uint32_t> current_active_servers;
        std::vector<std::unique_ptr<Hermes::Stub>> server_stubs;
        grpc::CompletionQueue broadcast_queue;
        {
            std::shared_lock<std::shared_mutex> server_state_lock {server_state_mutex};
            current_active_servers.resize(_active_servers.size());
            std::copy(_active_servers.begin(), _active_servers.end(), current_active_servers.begin());
            current_epoch = epoch;
            // current_active_servers = _active_servers.copy();
        }
        auto start = std::chrono::system_clock::now();
        for (auto& server: current_active_servers) {
            std::string addr = "localhost:" + std::to_string(server);
            server_stubs.push_back(create_stub(addr));
        }
        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
        SPDLOG_LOGGER_TRACE (logger, "Took {} us to create grpc stubs", duration);
        broadcast_invalidate(write_ts, value, key, broadcast_queue, current_active_servers, server_stubs, current_epoch);

        //// To test write replay
        //if (server_id == 50052) {
        //    terminate();
        //    return;
        //}

        // Check if the write was interrupted by a higher priority write
        if (!hermes_val->is_write()) {
            // TODO(): This shouldn't be required. Just return
            SPDLOG_LOGGER_INFO(logger, "[{}]::Received Invalidate RPC in the middle of write RPC. Aborting write.", get_tid());
            broadcast_queue.Shutdown();
            break;
        }

        // Wait till all the acks for the invalidate arrives 
        auto res = receive_acks(broadcast_queue, key, current_active_servers.size());
        int acks = res.first;
        int acceptances = res.second;
    
        //if (acceptances == _stubs.size()) {
        if (acceptances == current_active_servers.size()) {
            SPDLOG_LOGGER_DEBUG(logger, "[{}]::Received all acceptances for key: {}", get_tid(), key);
            // Value was accepted by all the nodes, we can trasition safely back to valid state
            // and propagate a VAL message to all the nodes. Wait till we get ACKs back (do we need this??)
            // auto thread = std::thread(std::bind(&HermesServiceImpl::broadcast_validate, this, hermes_val->timestamp, key));
            // {
            //     std::unique_lock<std::mutex> server_state_lock {server_state_mutex};
                
            // }
            broadcast_validate(hermes_val->timestamp, key, current_active_servers, server_stubs);
            hermes_val->coord_write_to_valid_transition();
            break;
        }
        else {
            // backoff
            //std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void HermesServiceImpl::performWriteReplay(HermesValue *hermes_val) {
    SPDLOG_LOGGER_TRACE (logger, "[{}]::performing write replay", get_tid());
    hermes_val->fol_replay_to_write_transition();
    performWrite(hermes_val);
}

bool HermesServiceImpl::isCoordinator(HermesValue *hermes_val) {
    // If the key is in WRITE state, the current node must be the coordinator
    return hermes_val->getState() == State::WRITE;
}

grpc::Status HermesServiceImpl::Read(grpc::ServerContext *ctx, 
        const ReadRequest *req, ReadResponse *resp) {
    if (!dead.load()) {
        std::string key = req->key();

        SPDLOG_LOGGER_INFO(logger, "[{}]::Received Read Request!", get_tid());
        SPDLOG_LOGGER_DEBUG(logger, "key: {}", key);

        std::pair<bool, map_iterator> is_present = isKeyPresent(key);

        if (is_present.first) {
            SPDLOG_LOGGER_DEBUG (logger, "[{}]::Read::Key found!", get_tid());
            // TODO
            const auto hermes_val = is_present.second->second.get();
            //hermes_val->wait_till_valid();
            while (true) {
                if (!hermes_val->wait_till_valid_or_timeout(replay_timeout)) {
                    SPDLOG_LOGGER_DEBUG (logger, "[{}]::Read::replay timeout expired", get_tid());
                    if (!isCoordinator(hermes_val)) {
                        // replay timeout expired. Start write replay
                        hermes_val->fol_invalid_to_replay_transition();
                        performWriteReplay(hermes_val);
                        break;
                    }
                    else {
                        SPDLOG_LOGGER_DEBUG (logger, "[{}]::Read::current node is the coordinator so cannot start write replay", get_tid());
                    }
                }
                else {
                    break;
                }
            }
            // perform the read corresponding to the current request
            resp->set_value(hermes_val->value);
            return grpc::Status::OK;
        }
        else {
            SPDLOG_LOGGER_DEBUG (logger, "[{}]::Read::Key not found!", get_tid());
            std::string not_found = "Key not found";
            resp->set_value(not_found);
            return grpc::Status::OK;
        }
    }
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server is down");
}

grpc::Status HermesServiceImpl::Write(grpc::ServerContext *ctx, const WriteRequest *req, Empty *resp) {
    if (!dead.load()) {
        std::string key = req->key();
        std::string value = req->value();

        SPDLOG_LOGGER_INFO(logger, "[{}]::Received Write Request!", get_tid());
        SPDLOG_LOGGER_DEBUG(logger, "key: {}", key);
        SPDLOG_LOGGER_DEBUG(logger, "value: {}", value);

        HermesValue *hermes_val;
        std::pair<bool, map_iterator> is_present = isKeyPresent(key);
        if (is_present.first) {
            SPDLOG_LOGGER_DEBUG (logger, "[{}]::Write::Key found!", get_tid());
            hermes_val = is_present.second->second.get();
        }
        else {
            SPDLOG_LOGGER_DEBUG (logger, "[{}]::Write::Key not found!", get_tid());
            hermes_val = writeNewKey(key, value);
        }

        // Stall writes till we are sure that the key is valid
        //hermes_val->wait_till_valid();
        while(true) {
            if (!hermes_val->wait_till_valid_or_timeout(replay_timeout)) {
                SPDLOG_LOGGER_DEBUG (logger, "[{}]::Write::replay timeout expired", get_tid());
                if (!isCoordinator(hermes_val)) {
                    // replay timeout expired. Start write replay for the invalid key
                    hermes_val->fol_invalid_to_replay_transition();
                    performWriteReplay(hermes_val);
                    break;
                }
                else {
                    SPDLOG_LOGGER_DEBUG (logger, "[{}]::Write::current node is the coordinator so cannot start write replay", get_tid());
                }
            }
            else {
                break;
            }
        }

        // perform the write corresponding to the current request
        hermes_val->coord_valid_to_write_transition(value, server_id);
        performWrite(hermes_val);

        return grpc::Status::OK;
    }
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server is down");
}

void HermesServiceImpl::broadcast_invalidate(Timestamp &ts, const std::string &value, std::string &key, 
        grpc::CompletionQueue &cq, std::vector<uint32_t> &servers,
        std::vector<std::unique_ptr<Hermes::Stub>> &server_stubs, uint32_t epoch) {   
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasting INVALIDATE RPCs for key {}", get_tid(), key);
    //int num_other_servers = _stubs.size();
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(mlt);

    grpc::Alarm alarm;
    int alarm_tag = -1;
    GrpcAsyncCall<InvalidateResponse>* alarm_call = new GrpcAsyncCall<InvalidateResponse>(alarm_tag);
    alarm.Set(&cq, deadline, (void*)alarm_call);
    //alarm.Set(&cq, deadline, reinterpret_cast<void*>(&alarm_tag));

    uint64_t i = 0;
    HermesTimestamp grpc_ts = ts.get_grpc_timestamp();

    // Get the list of currently active servers in the cluster
    // std::unordered_set<uint32_t> active_servers = _active_servers.copy();

    // Expect acks from all the active servers. If a server goes down, this set is updated in the mayday RPC
    // pending_acks = ThreadSafeUnorderedSet<uint32_t> {active_servers};

    //for (auto& stub: _stubs) {
    for (auto& server: servers) {
        SPDLOG_LOGGER_TRACE(logger, "[{}]::sending invalidate to node_id: {}, for key {} with grpc_tag={}", get_tid(), server, key, i);
        // Send invalidates
        InvalidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        req.set_value(value);
        req.set_epoch_id(epoch);
        GrpcAsyncCall<InvalidateResponse>* call = new GrpcAsyncCall<InvalidateResponse>(i);

        auto receiver = server_stubs[i]->AsyncInvalidate(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasted Invalidate RPCs", get_tid());
}

std::pair<int, int> HermesServiceImpl::receive_acks(grpc::CompletionQueue &cq, std::string key, uint32_t num_servers) {
    int acks_received = 0;
    int acceptances_received = 0;
    int alarm_tag;
    // Don't use the set of servers since it might be modified by a parallel thread and we don't want locks here
    // num_servers = _stubs.size();
    alarm_tag = -1;
    void* next_tag;
    bool ok;

    while (cq.Next(&next_tag, &ok)) {
        if (ok) {
            GrpcAsyncCall<InvalidateResponse>* grpc_tag = static_cast<GrpcAsyncCall<InvalidateResponse>*>(next_tag);
            if (grpc_tag->tag_value == alarm_tag) {
                // MLT expired. Return from this function and keep retrying...
                SPDLOG_LOGGER_INFO(logger, "[{}]::Alarm expired while broadcasting", get_tid());
                break;
            } else {
                // Get the node_id of the node which sent the ACK and erase that entry from the pending_acks set
                uint32_t responder = grpc_tag->response.responder();
                SPDLOG_LOGGER_TRACE(logger, "[{}]::grpc_tag={}::received ACK from {} for key {}", get_tid(), grpc_tag->tag_value, responder, key);
                // pending_acks.erase(responder);
                acks_received++;
                if (grpc_tag->response.accept()) {
                    SPDLOG_LOGGER_TRACE(logger, "[{}]::validate accepted by {} for key {}", get_tid(), responder, key);
                    acceptances_received++;
                }
                if (acks_received == num_servers) {
                // if (pending_acks.empty()) {
                    break;
                }
            }
        } else {
            SPDLOG_LOGGER_CRITICAL(logger, "[{}]::Not okay!", get_tid());
        }
        // delete grpc_tag;
    }
    cq.Shutdown();
    SPDLOG_LOGGER_INFO(logger, "[{}]::Received " + std::to_string(acks_received) + " acks", get_tid());
    SPDLOG_LOGGER_INFO(logger, "[{}]::Received " + std::to_string(acceptances_received) + " acceptances", get_tid());
    return std::make_pair<>(acks_received, acceptances_received);
}

void HermesServiceImpl::broadcast_validate(Timestamp ts, std::string key, std::vector<uint32_t> &servers, 
        std::vector<std::unique_ptr<Hermes::Stub>> &server_stubs) {
    grpc::CompletionQueue cq;
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasting VALIDATE RPCs", get_tid());
    //int num_other_servers = _stubs.size();

    HermesTimestamp grpc_ts = ts.get_grpc_timestamp();

    // Get the list of currently active servers in the cluster
    // std::unordered_set<uint32_t> active_servers = _active_servers.copy();

    // Expect acks from all the active servers. If a server goes down, this set is updated in the mayday RPC
    // pending_acks = ThreadSafeUnorderedSet<uint32_t> {active_servers};

    //for (auto& stub: _stubs) {
    uint64_t i = 0;
    for (auto& server: servers) {
        SPDLOG_LOGGER_TRACE(logger, "[{}]::sending VALIDATE to node_id: {}, for key {}", get_tid(), server, key);
        // auto& stub = _stubs[server];
        ValidateRequest req;
        req.set_key(key);
        req.set_allocated_ts(&grpc_ts);
        GrpcAsyncCall<Empty>* call = new GrpcAsyncCall<Empty>(i);

        auto receiver = server_stubs[i]->AsyncValidate(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        grpc_ts = *(req.release_ts());
        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasted validate RPCs", get_tid());
    // Dont wait for responses
    cq.Shutdown();
}

// Invalidate handling via gRPC
grpc::Status HermesServiceImpl::Invalidate(grpc::ServerContext *ctx, const InvalidateRequest *req, InvalidateResponse *resp) {
    HermesTimestamp ts = req->ts();
    SPDLOG_LOGGER_INFO(logger, "[{}]::Received Invalidate RPC from node_id: {} for key {}", get_tid(), Timestamp(ts).node_id, req->key());
    // Send the node_id so that the receiver knows which node send the ack
    resp->set_responder(server_id);
    
    if (req->epoch_id() != epoch) {
        // Epoch id doesnt match. Reject request
        SPDLOG_LOGGER_DEBUG(logger, "[{}]::Rejecting invalidate request because received epoch_id {} doesn't match with local epoch id {}", get_tid(), req->epoch_id(), epoch);
        resp->set_accept(false);
        return grpc::Status::OK;
    }
    auto value = req->value();
    HermesValue* hermes_val {nullptr};
    bool new_key = false;

    std::pair<bool, map_iterator> is_present = isKeyPresent(req->key());
    if (is_present.first) {
        SPDLOG_LOGGER_DEBUG (logger, "[{}]::Invalidate::Key found!", get_tid());
        hermes_val = is_present.second->second.get();
    }
    else {
        SPDLOG_LOGGER_DEBUG (logger, "[{}]::Invalidate::Key not found!", get_tid());
        hermes_val = writeNewKey(req->key(), value);
        new_key = true;
    }

    // Reject any key that has lower timestamp
    if (!new_key && hermes_val->is_lower(ts)) {
        // Timestamp is lower than local timestamp. Reject
        resp->set_accept(false);
        SPDLOG_LOGGER_DEBUG(logger, "[{}]::Rejecting invalidate request because received timestamp {} is lower than local timestamp {}", get_tid(), Timestamp(ts).toString(), hermes_val->timestamp.toString());
        return grpc::Status::OK;
    }
    hermes_val->fol_invalidate(value, ts);
    SPDLOG_LOGGER_INFO(logger, "[{}]::Accepting Invalidate RPC for key {}", get_tid(), req->key());
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
    SPDLOG_LOGGER_INFO(logger, "[{}]::Received validate RPC from node_id: {} for key {}", get_tid(), Timestamp(ts).node_id, req->key());
    auto& key = req->key();
    HermesValue* hermes_val = getValueFromDB(req->key())->second.get();
    
    if (hermes_val->not_equal(ts)) {
        // Timestamp is not equal to local timestamp, which means a request with higher timestamp must 
        // have been accepted. Ignore
        SPDLOG_LOGGER_INFO(logger, "[{}]::Rejecting validate RPC from node_id: {} for key {}", get_tid(), Timestamp(ts).node_id, req->key());
        return grpc::Status::OK;
    }
    hermes_val->fol_invalid_to_valid_transition();
    SPDLOG_LOGGER_DEBUG(logger, "[{}]::Validated key {} after write", get_tid(), key);
    return grpc::Status::OK;
}

// Called (by?) the server which is going down
grpc::Status HermesServiceImpl::Mayday(grpc::ServerContext *ctx, const MaydayRequest *req, Empty *resp) {
    uint32_t failing_node = req->node_id();
    SPDLOG_LOGGER_CRITICAL(logger, "[{}]::node_id {} failed", get_tid(), failing_node);
    // _active_servers.erase(failing_node);
    // This server will not receive an ACK, if it was expecting one, from the failing node.
    // pending_acks.erase(failing_node);
    {
        std::unique_lock<std::shared_mutex> server_state_lock {server_state_mutex};
        SPDLOG_LOGGER_DEBUG(logger, "[{}]::lock acquired to modify membership list", get_tid());
        for (auto it = _active_servers.begin(); it!= _active_servers.end(); it++) {
            if (*it == failing_node) {
                SPDLOG_LOGGER_DEBUG(logger, "[{}]::removing failed node_id {}", get_tid(), *it);
                _active_servers.erase(it);
                break;
            }
        }
        SPDLOG_LOGGER_DEBUG(logger, "[{}]::old epoch is {}, new epoch is {}", get_tid(), epoch, req->epoch_id());
        epoch = req->epoch_id();
    }
    return grpc::Status::OK;
}

void HermesServiceImpl::broadcast_mayday(grpc::CompletionQueue &cq) {
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasting Mayday RPCs", get_tid());
    //int num_other_servers = _stubs.size();

    uint64_t i = 0;
    
    // Get the list of currently active servers in the cluster
    // std::unordered_set<uint32_t> active_servers = _active_servers.copy();
    std::vector<uint32_t> active_servers(_active_servers.size());
    std::copy(_active_servers.begin(), _active_servers.end(), active_servers.begin());

    //for (auto& stub: _stubs) {
    for (auto& server: active_servers) {
        auto& stub = _stubs[server];
        MaydayRequest req;
        req.set_node_id(server_id);
        req.set_epoch_id(epoch+1);
        GrpcAsyncCall<Empty>* call = new GrpcAsyncCall<Empty>(i);

        auto receiver = stub->AsyncMayday(&call->ctx, req, &cq);
        receiver->Finish(&call->response, &call->status, (void*)call);

        i++;
    }
    SPDLOG_LOGGER_INFO(logger, "[{}]::Broadcasted Mayday RPCs", get_tid());
}

void HermesServiceImpl::receive_mayday_acks(grpc::CompletionQueue &cq) {
    int acks_received = 0;
    int num_servers;
    // Don't use the set of servers since it might be modified by a parallel thread and we don't want locks here
    //num_servers = _stubs.size();
    num_servers = _active_servers.size();
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
        //receive_mayday_acks(mayday_queue);
        mayday_queue.Shutdown();
        dead.store(true);
    }
    else {
        SPDLOG_LOGGER_CRITICAL(logger, "not implemented");
    }
}

grpc::Status HermesServiceImpl::Heartbeat(grpc::ServerContext *ctx, const Empty *req, Empty *resp) {
    return grpc::Status::OK;
}
