#include "wal.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>
#include <chrono>
#include <functional>
#include <iostream>

#define FS_BLOCK_SIZE 4096

#define A 54059 /* a prime */
#define B 76963 /* another prime */
#define C 86969 /* yet another prime */
#define FIRSTH 37 /* also prime */

/**
 * Hash function based on xor. Reference: https://stackoverflow.com/questions/8317508/hash-function-for-a-string
 */
uint64_t key_hash(const std::string *key) {
    uint64_t h = FIRSTH;
    for (int i=0; i<key->size(); i++) {
        h = (h * A) ^ (key->at(i) * B);
    }
    return h;
}

WAL_writer::WAL_writer(std::string &file_prefix, uint32_t partition_num,
        std::shared_ptr<spdlog::logger> &logger): 
        file_prefix(file_prefix), partition_num(partition_num) {
    write_offset = 0ll;
    int ret = posix_memalign(&staging, FS_BLOCK_SIZE, FS_BLOCK_SIZE);    // Pointer should be aligned to block size for O_DIRECT
    if (ret) {
        throw new std::runtime_error("Memory allocation failed");
    }
    open_log_file();
}

std::string WAL_writer::open_log_file() {
    std::string file_name = file_prefix + "_part_" + std::to_string(partition_num) + "_" + std::to_string(file_cntr);
    // SPDLOG_LOGGER_WARN(logger, "Opening WAL log file {}", file_name);
    std::cout << "Opening WAL log file " << file_name << "\n";
    fd = open(file_name.c_str(), O_DIRECT | O_APPEND | O_CREAT | O_WRONLY, 0666);
    if (fd == -1) {
        throw new std::runtime_error("Unable to open file for WAL");
    }
    file_cntr++;
    return file_name;
}

std::string WAL_writer::append(const std::string *key, const std::string *value) {
    std::unique_lock<std::mutex> write_lock(write_mutex);
    // std::cout << "Appending to WAL file\n";
    char* temp_staging = reinterpret_cast<char*>(staging);
    auto key_size = key->size();
    memcpy(temp_staging, reinterpret_cast<void*>(&key_size), sizeof(key_size));
    memcpy(temp_staging + sizeof(key_size), reinterpret_cast<const void*>(key->c_str()), key_size);
    auto value_size = value->size();
    temp_staging = temp_staging + (2<<10);
    memcpy(temp_staging, reinterpret_cast<void*>(&value_size), sizeof(value_size));
    memcpy(temp_staging + sizeof(value_size), reinterpret_cast<const void*>(value->c_str()), value_size);
    temp_staging = temp_staging + (2<<10) - sizeof(lsn);
    memcpy(temp_staging, reinterpret_cast<void*>(&lsn), sizeof(lsn));

    auto start = std::chrono::system_clock::now();
    int bytes_written = write(fd, staging, STAGING_SIZE);
    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    printf("Time taken to write to log: %ld us", duration);
    // if (bytes_written == -1) {
    //     std::cout << "Write failed\n";
    // }
    write_offset += STAGING_SIZE;
    if (write_offset == CAPACITY) {
        // std::cout << "Closing WAL log file\n";
        write_offset = 0ll;
        close(fd);
        return open_log_file();
    }
    lsn++;
    // std::cout << "Appended to WAL file\n";
    return std::string();
}


StorageHelper::StorageHelper(std::string &log_path, std::string &db_path,
        std::shared_ptr<spdlog::logger> &logger, uint32_t server_id): 
        db_path(db_path),
        logger(logger) {
    std::string log_prefix = log_path + "/" + "WAL_server_" + std::to_string(server_id);
    for (int i=0; i<NUM_WRITERS; i++) {
        log_writers.emplace_back(std::move(std::make_unique<WAL_writer>(log_prefix, i, logger)));
    }
    enable_snapshots.store(true);
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB *db_ptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_ptr);
    db.reset(db_ptr);
    assert(status.ok());
    snapshotting_thread = std::thread(std::bind(&StorageHelper::take_snapshots, this));
}

void StorageHelper::write_log(const std::string *key, const std::string *value) {
    // SPDLOG_LOGGER_TRACE(logger, "Writing log record for key: {}", key->c_str());
    // std::cout << "Writing log record for key: " << key << "\n";
    uint32_t h = key_hash(key) % NUM_WRITERS;
    auto ret = log_writers[h]->append(key, value);
    if (ret.size()) {
        std::unique_lock<std::mutex> write_lock(file_queue_mutex);
        pending_wal_files.push_back(ret);
    }
}

StorageHelper::~StorageHelper() {
    enable_snapshots.store(false);
    snapshotting_thread.join();
    db->Close();
}

void StorageHelper::take_snapshots() {
    while (enable_snapshots.load()) {
        // SPDLOG_LOGGER_DEBUG(logger, "Taking snapshot...");
        std::this_thread::sleep_for(std::chrono::minutes(2));
        std::cout << "Taking snapshot\n";
        std::vector<std::string> file_names;
        {
            std::unique_lock<std::mutex> write_lock(file_queue_mutex);
            file_names.resize(pending_wal_files.size());
            std::copy(pending_wal_files.begin(), pending_wal_files.end(), file_names.begin());
        }
        // auto key_ptr = 
        void *record;
        posix_memalign(&record, 64, WAL_writer::STAGING_SIZE);
        for (auto file_name: file_names) {
            //open log file
            int fd = open(file_name.c_str(), O_RDONLY);
            uint64_t key_length, value_length;
            
            char *key; char *value;
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            for (int i=0; i<(WAL_writer::CAPACITY/WAL_writer::STAGING_SIZE); i++) {
                read(fd, record, WAL_writer::STAGING_SIZE);
                memcpy(reinterpret_cast<void*>(&key_length), 
                        reinterpret_cast<char*>(record), sizeof(key_length));
                key = reinterpret_cast<char*>(record) + sizeof(key_length);
                memcpy(reinterpret_cast<void*>(&value_length), 
                        reinterpret_cast<char*>(record) + WAL_writer::STAGING_SIZE/2, sizeof(value_length));
                value = reinterpret_cast<char*>(record) + WAL_writer::STAGING_SIZE/2 + sizeof(value_length);
                rocksdb::Slice key_slice {reinterpret_cast<const char*>(key), key_length};
                rocksdb::Slice value_slice {reinterpret_cast<const char*>(value), value_length};
                db->Put(options, key_slice, value_slice);
            }
            // close log file
            close(fd);
        }
        // SPDLOG_LOGGER_DEBUG(logger, "Snapshot taken...");
        std::cout << "Snapshot taken\n";
    }
}