#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <rocksdb/db.h>
#include "spdlog/include/spdlog/spdlog.h"
#include "spdlog/include/spdlog/sinks/basic_file_sink.h"

/**
 * Format:
 * - 2KB (key size + key)
 * - 2KB (value size + value)
 * - LSN (8 bytes)
 */

class WAL_writer {
public:
    WAL_writer(std::string &file_prefix, uint32_t partition, std::shared_ptr<spdlog::logger> &logger);

    // returns name of log file if it is has been completely written to
    std::string append(const std::string *key, const std::string *value);

    static const size_t CAPACITY = 512 * (1<<10); // 512 kb

    static const size_t STAGING_SIZE = 4 * (1<<10); // 4kb
private:
    void *ptr {nullptr};

    void *staging {nullptr};

    size_t write_offset;

    uint32_t file_cntr;

    uint64_t lsn;

    std::string file_prefix;

    int fd;

    std::string open_log_file();

    uint32_t partition_num;

    std::mutex write_mutex;

    std::shared_ptr<spdlog::logger> logger;
};

class StorageHelper {
public:
    StorageHelper(std::string &log_path, std::string &db_path, std::shared_ptr<spdlog::logger> &logger,
        uint32_t server_id);

    ~StorageHelper();

    void write_log(const std::string *key, const std::string *value);

    static const uint32_t NUM_WRITERS = 1;

private:
    std::string db_path;

    std::vector<std::unique_ptr<WAL_writer>> log_writers;

    std::thread snapshotting_thread;

    std::atomic<bool> enable_snapshots;

    std::vector<std::string> pending_wal_files;

    std::unique_ptr<rocksdb::DB> db;

    void take_snapshots();

    std::mutex file_queue_mutex;

    std::shared_ptr<spdlog::logger> logger;
};