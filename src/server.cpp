#include "server.h"
#include <alarm.h>

std::unique_ptr<Hermes::Stub> create_stub(const std::string &addr) {
    auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return std::make_unique<Hermes::Stub>(channel_ptr);
}

HermesServiceImpl::HermesServiceImpl(uint32_t id): server_id(id) {
}

grpc::Status HermesServiceImpl::Read(grpc::ServerContext *ctx, 
        ReadRequest *req, ReadResponse *resp) {
    auto key = req->key();
    if (key_value_map.find(key) == key_value_map.end()) {
        return grpc::Status::OK;
    }

    auto it = key_value_map.find(key);
    const auto hermes_val = it->second.get();
    std::unique_lock lock {hermes_val->stall_mutex};
    // Wait till the key is in VALID state
    hermes_val->stall_cv.wait(lock, hermes_val->st == VALID);
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

    {
        // State transition to WRITE
        std::unique_lock<std::mutex> lock(hermes_val->stall_mutex);
        // Wait till the key is in VALID state
        hermes_val->stall_cv.wait(lock, hermes_val->st == VALID);
        hermes_val->increment_ts(server_id);
        hermes_val->st = WRITE;
    }

    // Send invalid messages to all nodes
    invalidate_value(hermes_val, key);
}

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
}

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