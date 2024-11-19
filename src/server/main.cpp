#include <iostream>
#include "server.h"

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
