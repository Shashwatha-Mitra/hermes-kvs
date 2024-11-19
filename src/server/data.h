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
    std::string key;
    std::string value;
    std::condition_variable stall_cv;
    std::mutex stall_mutex;
    Timestamp timestamp;
    State st;
    // StateClass st_cl;

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
    
    // Helper function that waits till the condition variable is notified
    void wait_till_valid() {
        std::unique_lock<std::mutex> lock(stall_mutex);

        // Wait till the key is in VALID state
        stall_cv.wait(lock, st == VALID);
    }

    // Helper function waits for acks for the INV messages
    void wait_for_acks() {
        // TODO: Copy from the invalidate_value function
        return;
    }
    
    void is_valid() {
        return st == VALID;
    }

    // Need server context here
    void coord_valid_to_write_transition() {
        // TODO: Valid State class handles the work done here
        // Steps to be completed atomically: update timestamp, update KV store, broadcast invalidates.
        // Wait till everything is complete
        this->st = WRITE;
    }

    // Need server context here
    void coord_write_to_valid_transition() {
        // TODO: Write State class handles the work done here
        // Steps to be completed atomically: Broadcast value, ts. Wait for ACKs (how to handle failure here??)
        // Wait till everything is complete
        this->st = VALID;
    }

    void coord_write_to_trans_transition() {
        // TODO: Add locks around st to ensure atomic update
        this->st = TRANSIENT;
    }

    void coord_trans_to_invalid_transition() {
        // TODO: Add locks around st to ensure atomic update
        this->st = INVALID;
    }

    void fol_invalid_to_replay_transition() {
        // TODO: Implement this later when dealing with failure.
        this->st = REPLAY;
    }

    void coord_invalid_to_valid_transition() {
        // TODO: Invalid State class handles the work done here
        this->st = VALID;
    }

    void fol_replay_to_write_transition() {
        // TODO: Implement this later when dealing with failure
        this->st = WRITE;
    }


    void fol_valid_to_invalid_transition() {
        // TODO: Valid State class handles the work done here
        // Steps to be completed atomically: Update local ts if valid, Send ACKs
        // Wait till everything is complete
        this->st = INVALID;
    }

    void fol_invalid_to_valid_transition() {
        // Calling the co-ordinator code here since the behaviour is the same
        coord_invalid_to_valid_transition();
    }
        
    void fol_invalid_to_invalid_transition() {
        // TODO: Check if the timestamp is greater than the current timestamp. If yes update the timestamp
        this->st = INVALID;
    }
};
