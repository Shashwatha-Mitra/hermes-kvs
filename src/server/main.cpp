#include <iostream>
#include "server.h"
#include "config.h"

ABSL_FLAG(uint32_t, id, 1, "Server id");
ABSL_FLAG(uint32_t, port, 50050, "Port");
ABSL_FLAG(std::string, log_dir, "", "log directory");
ABSL_FLAG(std::string, config_file, "", "Config file");

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    uint32_t id = absl::GetFlag(FLAGS_id);
    uint32_t port = absl::GetFlag(FLAGS_port);
    std::string log_dir = absl::GetFlag(FLAGS_log_dir);
    std::string server_address("localhost:" + std::to_string(port));
    std::string config_file = absl::GetFlag(FLAGS_config_file);
    auto server_list = parseConfigFile(config_file);

    HermesServiceImpl service(id, log_dir, server_list, port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}