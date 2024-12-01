#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include "hermes.grpc.pb.h"
#include "state.h"

#include <vector>
#include <shared_mutex>
#include <string>
#include <memory>
#include <thread>
#include <cstdint>
#include <unordered_map>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include "spdlog/include/spdlog/spdlog.h"
#include "spdlog/include/spdlog/sinks/basic_file_sink.h"

#include "../utils/threadsafe_unordered_set.h"

class HermesServiceImpl: public Hermes::Service {
private:
    using InvalidateRespReader = typename std::unique_ptr<grpc::ClientAsyncResponseReader<InvalidateResponse>>;

    //std::vector<std::unique_ptr<Hermes::Stub>> active_server_stubs;
    std::unordered_map<uint32_t, std::unique_ptr<Hermes::Stub>> _stubs;

    // ThreadSafeUnorderedSet<uint32_t> _active_servers;
    //std::vector<std::string> active_servers;
    std::vector<uint32_t> _active_servers;
    
    ThreadSafeUnorderedSet<uint32_t> pending_acks;

    std::string self_addr;

    std::shared_mutex server_state_mutex; // Mutex to lock server stubs and server names

    std::shared_mutex hashmap_mutex;

    std::atomic<bool> dead;

    const uint32_t mlt = 1; // Message loss timeout in seconds

    const uint32_t replay_timeout = 1; // Time to wait before calling replay

    uint32_t epoch;

    uint32_t server_id;

    std::unordered_map<std::string, std::unique_ptr<HermesValue>> key_value_map;

    std::unordered_map<std::string, bool> is_coord_for_key;

    std::shared_ptr<spdlog::logger> logger;


    void invalidate_value(HermesValue *val, std::string &key);

    void broadcast_invalidate(Timestamp &ts, const std::string &value, std::string &key, 
        grpc::CompletionQueue &cq, std::vector<uint32_t> &servers, 
        std::vector<std::unique_ptr<Hermes::Stub>> &server_stubs);

    void broadcast_validate(Timestamp ts, std::string key, std::vector<uint32_t> &servers, 
        std::vector<std::unique_ptr<Hermes::Stub>> &server_stubs);

    void broadcast_mayday(grpc::CompletionQueue &cq);

    std::pair<int, int> receive_acks(grpc::CompletionQueue &cq, std::string key, uint32_t num_servers);

    void receive_mayday_acks(grpc::CompletionQueue &cq);

    inline uint32_t portToID(uint32_t port);

    uint32_t addrToID(std::string& addr);

public:
    HermesServiceImpl(uint32_t id, std::string &log_dir, const std::vector<std::string> &server_list, uint32_t port, std::atomic<bool>& terminate_flag);

    grpc::Status Read(grpc::ServerContext *ctx, const ReadRequest *req, ReadResponse *resp) override;

    grpc::Status Write(grpc::ServerContext *ctx, const WriteRequest *req, Empty *resp) override;

    grpc::Status Invalidate(grpc::ServerContext *ctx, const InvalidateRequest *req, InvalidateResponse *resp) override;

    grpc::Status Validate(grpc::ServerContext *ctx, const ValidateRequest *req, Empty *resp) override;

    grpc::Status Mayday(grpc::ServerContext *ctx, const MaydayRequest *req, Empty *resp) override;

    grpc::Status Terminate(grpc::ServerContext *ctx, const TerminateRequest *req, Empty *resp) override;

    void terminate(bool graceful = true);

    virtual ~HermesServiceImpl();
};

// ABSL_FLAG(uint32_t, id, 1, "Server id");
// ABSL_FLAG(uint32_t, port, 50050, "Port");
// ABSL_FLAG(std::string, log_dir, "", "log directory");

// int main(int argc, char** argv) {
//     absl::ParseCommandLine(argc, argv);
//     uint32_t id = absl::GetFlag(FLAGS_id);
//     uint32_t port = absl::GetFlag(FLAGS_port);
//     std::string log_dir = absl::GetFlag(FLAGS_log_dir);
//     std::string server_address("localhost:" + std::to_string(port));

//     HermesServiceImpl service(id, log_dir);

//     grpc::ServerBuilder builder;
//     builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//     builder.RegisterService(&service);
//     std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
//     std::cout << "Server listening on " << server_address << std::endl;
//     server->Wait();
// }
