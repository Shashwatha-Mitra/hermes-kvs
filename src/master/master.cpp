#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>

#include "master.h"

std::unique_ptr<Hermes::Stub> create_stub(const std::string &addr) {
    auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return std::make_unique<Hermes::Stub>(channel_ptr);
}

Master::Master(uint32_t id, std::string &log_dir, 
        const std::vector<std::string> &server_list
        )
        : server_id(id), stop(false), epoch(0) {
    // Logger initialization
    std::string log_file_name = log_dir + "/spdlog_master_" + std::to_string(id) + ".log";

    // Initialize the logger and set the flush rate
    //spdlog::flush_every(std::chrono::microseconds(100));
    spdlog::flush_every(std::chrono::milliseconds(1));
    logger = spdlog::basic_logger_mt("server_logger", log_file_name);

    // Set logging level
    logger->set_level(spdlog::level::trace);
    logger->flush_on(spdlog::level::trace);
    
    for (auto server: server_list) {
        uint32_t other_id = addrToID(server);
        SPDLOG_LOGGER_INFO(logger, "Adding {} to active list", other_id);
        _active_servers.insert(other_id);
        _stubs[other_id] = create_stub(server);
    }
}

void Master::start() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        sendHeartbeats();
    }
}

inline uint32_t Master::portToID(uint32_t port) {
    return port;
}

uint32_t Master::addrToID(std::string& addr) {
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

void Master::sendHeartbeats() {
    std::unordered_set<uint32_t> active_servers = _active_servers.copy();
    std::unordered_set<uint32_t> failed_servers;

    // Send heartbeats and detect failed servers
    for (auto server: active_servers) {
        auto& stub = _stubs[server];
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(150);
        grpc::ClientContext ctx;
        ctx.set_deadline(deadline);
        Empty req;
        Empty resp;
        SPDLOG_LOGGER_TRACE(logger, "sending heartbeat to node_id {}", server);
        grpc::Status status = stub->Heartbeat(&ctx, req, &resp);
        if (status.ok()) {
            SPDLOG_LOGGER_TRACE(logger, "node_id {} is running", server);
        }
        else {
            std::stringstream ss;
            ss << "Code " << status.error_code() << " Message " << status.error_message();
            SPDLOG_LOGGER_TRACE(logger, "node_id {} has failed. {}", server, ss.str());
            failed_servers.insert(server);
            _active_servers.erase(server);
        }
    }

    for (auto server: failed_servers) {
        epoch++;
        reconfigure(server);
    }
}

void Master::reconfigure(uint32_t server, bool fail) {
    // Reconfigure the hermes cluster due to membership change
    std::unordered_set<uint32_t> active_servers = _active_servers.copy();

    if (fail) {
        for (auto active: active_servers) {
            SPDLOG_LOGGER_TRACE(logger, "sending mayday to {}", active);
            auto& stub = _stubs[active];
            grpc::ClientContext ctx;
            MaydayRequest req;
            req.set_node_id(server);
            req.set_epoch_id(epoch);
            Empty resp;
            grpc::Status status = stub->Mayday(&ctx, req, &resp);
            assert(status.ok());
        }
    }
    else {
        // TODO: addition of new server
        assert(0);
    }
}
