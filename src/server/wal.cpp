#include "wal.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>
#include <chrono>
#include <functional>

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

WAL_writer::WAL_writer(std::string &file_prefix, uint32_t partition_num): file_prefix(file_prefix), partition_num(partition_num) {
    write_offset = 0ll;
    posix_memalign(&staging, 64, STAGING_SIZE);
}

void WAL_writer::open_log_file() {
    std::string file_name = file_prefix + "_" + std::to_string(file_cntr) + "_partition_" + std::to_string(partition_num);
    fd = open(file_name.c_str(), O_DIRECT | O_APPEND);
    if (fd == -1) {
        throw new std::runtime_error("Unable to open file for WAL");
    }
    file_cntr++;
}

std::string WAL_writer::append(const std::string *key, const std::string *value) {
    std::unique_lock<std::mutex> write_lock(write_mutex);
    
    void* temp_staging = staging;
    auto key_size = key->size();
    memcpy(temp_staging, reinterpret_cast<void*>(&key_size), sizeof(key_size));
    memcpy(temp_staging + sizeof(key_size), reinterpret_cast<const void*>(key->c_str()), key_size);
    auto value_size = value->size();
    temp_staging = temp_staging + (2<<10);
    memcpy(temp_staging, reinterpret_cast<void*>(&value_size), sizeof(value_size));
    memcpy(temp_staging + sizeof(value_size), reinterpret_cast<const void*>(value->c_str()), value_size);
    temp_staging = temp_staging + (2<<10) - sizeof(lsn);
    memcpy(temp_staging, reinterpret_cast<void*>(&lsn), sizeof(lsn));

    write(fd, staging, STAGING_SIZE);
    write_offset += STAGING_SIZE;
    if (write_offset == CAPACITY) {
        close(fd);
        open_log_file();
        return file_prefix + "_" + std::to_string(file_cntr-1);
    }
    lsn++;
    return std::string();
}


StorageHelper::StorageHelper(std::string &log_path, std::string &db_path,
        std::shared_ptr<spdlog::logger> &logger): 
        db_path(db_path),
        logger(logger) {
    std::string log_prefix = log_path + "/" + "hermes_wal";
    for (int i=0; i<NUM_WRITERS; i++) {
        log_writers.emplace_back(std::move(std::make_unique<WAL_writer>(log_path, i)));
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
        std::vector<std::string> file_names;
        {
            std::unique_lock<std::mutex> write_lock(file_queue_mutex);
            file_names.resize(pending_wal_files.size());
            std::copy(pending_wal_files.begin(), pending_wal_files.end(), file_names.begin());
        }
        for (auto file_name: file_names) {
            //open log file
            int fd = open(file_name.c_str(), O_RDONLY);
            uint32_t key_length, value_length;
            void *record;
            void *key; void *value;
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            for (int i=0; i<(WAL_writer::CAPACITY/WAL_writer::STAGING_SIZE); i++) {
                read(fd, record, WAL_writer::STAGING_SIZE);
                memcpy(reinterpret_cast<void*>(&key_length), record, sizeof(key_length));
                key = record + sizeof(key_length);
                memcpy(reinterpret_cast<void*>(&value_length), record + WAL_writer::STAGING_SIZE/2, sizeof(value_length));
                value = record + WAL_writer::STAGING_SIZE/2 + sizeof(value_length);
                rocksdb::Slice key_slice {reinterpret_cast<const char*>(key), key_length};
                rocksdb::Slice value_slice {reinterpret_cast<const char*>(value), value_length};
                db->Put(options, key_slice, value_slice);
            }
            // close log file
            close(fd);
        }
        std::this_thread::sleep_for(std::chrono::minutes(2));
    }
}