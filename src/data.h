#include "hermes.grpc.pb.h"
#include <condition_variable>
#include <mutex>

enum State {
    VALID,
    INVALID,
    WRITE,
    REPLAY,
    TRANSIENT
};

struct Timestamp {
    uint32_t logical_time;
    uint32_t node_id;

    Timestamp(HermesTimestamp ts) {
        this->logical_time = ts.local_ts();
        this->node_id = ts.node_id();
    }

    HermesTimestamp get_grpc_timestamp() {
        HermesTimestamp ts;
        ts.set_node_id(node_id);
        ts.set_local_ts(logical_time);
        return ts;
    }

    bool operator <(Timestamp& other) {
        if (this->logical_time != other.logical_time) {
            return this->logical_time < other.logical_time;
        }
        return this->node_id < other.node_id;
    }

    bool operator ==(Timestamp& other) {
        return this->node_id == other.node_id && this->logical_time == other.logical_time;
    }

    bool operator !=(Timestamp& other) {
        return this->node_id != other.node_id || this->logical_time != other.logical_time;
    }
};


struct HermesValue {
    std::string value;

    std::condition_variable stall_cv;

    std::mutex stall_mutex;

    Timestamp timestamp;

    State st;

    HermesValue(const std::string &value, uint32_t node_id) {
        this->value = value;
        timestamp.node_id = node_id;
        timestamp.logical_time = 0;
        st = VALID;
    }

    void increment_ts(uint32_t node_id) {
        timestamp.logical_time++;
        timestamp.node_id = node_id;
    }
};