#include "server.h"

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

    std::unique_lock<std::mutex> lock(hermes_val->stall_mutex);
    // Wait till the key is in VALID state
    hermes_val->stall_cv.wait(lock, hermes_val->st == VALID);
    hermes_val->increment_ts(server_id);
    hermes_val->st = WRITE;

    // Send invalid messages to all nodes
    invalidate_value(hermes_val, key);
}

void HermesServiceImpl::invalidate_value(HermesValue *val, std::string &key) {
    for (auto& stub: active_server_stubs) {
        // Send invalidates
        grpc::ClientContext ctx;
        InvalidateRequest req;
        InvalidateResponse resp;
        req.set_allocated_key(&key);
        // TODO: Fix this memory leak
        req.set_allocated_ts(&(val->timestamp.get_grpc_timestamp()));
        req.set_value(val->value);
        req.set_epoch_id(epoch);

        InvalidateResponse resp;
        auto status = stub->Invalidate(&ctx, req, &resp);
    }
    // Wait for response

    // Retry
}