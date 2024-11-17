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

    InvalidateRequest_Timestamp get_grpc_timestamp() {
        InvalidateRequest_Timestamp ts;
        ts.set_node_id(node_id);
        ts.set_local_ts(logical_time);
        return ts;
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
        st = WRITE;
    }

    void increment_ts(uint32_t node_id) {
        timestamp.logical_time++;
        timestamp.node_id = node_id;
    }
};