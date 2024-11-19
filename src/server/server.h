#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include "hermes.grpc.pb.h"
#include "data.h"

#include <vector>
#include <string>
#include <memory>
#include <thread>
#include <cstdint>
#include <unordered_map>
#include <grpcpp/grpcpp.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include "spdlog/include/spdlog/spdlog.h"
#include "spdlog/include/spdlog/sinks/basic_file_sink.h"

class HermesServiceImpl: public Hermes::Service {
private:
    std::vector<std::unique_ptr<Hermes::Stub>> active_server_stubs;

    std::vector<std::string> active_servers;

    const uint32_t mlt = 1; // Message loss timeout in seconds

    uint32_t epoch;

    uint32_t server_id;

    std::unordered_map<std::string, std::unique_ptr<HermesValue>> key_value_map;

    std::unordered_map<std::string, bool> is_coord_for_key;

    void invalidate_value(HermesValue *val, std::string &key);

    std::shared_ptr<spdlog::logger> logger;

public:
    HermesServiceImpl(uint32_t id);

    grpc::Status Read(grpc::ServerContext *ctx, ReadRequest *req, ReadResponse *resp);

    grpc::Status Write(grpc::ServerContext *ctx, WriteRequest *req, Empty *resp);

    grpc::Status Invalidate(grpc::ServerContext *ctx, InvalidateRequest *req, InvalidateResponse *resp);

    grpc::Status Validate(grpc::ServerContext *ctx, ValidateRequest *req, Empty *resp);

    virtual ~HermesServiceImpl(){}
};

ABSL_FLAG(uint32_t, id, 1, "Server id");
ABSL_FLAG(uint32_t, port, 50050, "Port");
ABSL_FLAG(std::string, log_dir, "", "log directory");

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    uint32_t id = absl::GetFlag(FLAGS_id);
    uint32_t port = absl::GetFlag(FLAGS_port);
    std::string log_dir = absl::GetFlag(FLAGS_log_dir);
    std::string server_address("localhost:" + std::to_string(port));

    HermesServiceImpl service(id, log_dir);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}
