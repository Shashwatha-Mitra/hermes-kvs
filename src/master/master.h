#pragma once
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include "hermes.grpc.pb.h"

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

class Master {
private:
    std::unordered_map<uint32_t, std::unique_ptr<Hermes::Stub>> _stubs;

    ThreadSafeUnorderedSet<uint32_t> _active_servers;
    
    ThreadSafeUnorderedSet<uint32_t> pending_acks;

    std::string self_addr;
    
    uint32_t server_id;

    uint32_t epoch;

    bool stop;

    std::shared_ptr<spdlog::logger> logger;

    void sendHeartbeats();
    void reconfigure(uint32_t server, bool fail=true);
    inline uint32_t portToID(uint32_t port);
    uint32_t addrToID(std::string& addr);

public:
    Master(uint32_t id, std::string &log_dir, const std::vector<std::string> &server_list);

    void start();
};