#include "hermes.grpc.pb.h"
#include <condition_variable>
#include <mutex>
#include <atomic>

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

    Timestamp() = default;

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
    std::atomic<State> st;

    HermesValue(const std::string &value, uint32_t node_id) {
        this->value = value;
        timestamp.node_id = node_id;
        timestamp.logical_time = 0;
        st.store(VALID, std::memory_order_release);
    }

    void increment_ts(uint32_t node_id) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        timestamp.logical_time++;
        timestamp.node_id = node_id;
    }
    
    // Helper function that waits till the condition variable is notified
    void wait_till_valid() {
        std::unique_lock<std::mutex> lock(stall_mutex);

        // Wait till the key is in VALID state
        stall_cv.wait(lock, [this] {return is_valid();});
    }

    // Helper function that waits till the condition variable is notified with an additional timeout
    // Timeout is in seconds
    void wait_till_valid_timeout(uint32_t timeout) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        
        // Wait till the key is in VALID state or timeout occurs
        stall_cv.wait_for(lock, std::chrono::seconds(timeout), [this] {return is_valid();});
    }

    bool is_lower(HermesTimestamp ts) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        return Timestamp(ts) < timestamp;
    }

    bool not_equal(HermesTimestamp ts) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        return Timestamp(ts) != timestamp;
    }
    
    inline bool is_valid() {
        return (st.load(std::memory_order_acquire) == VALID);
    }

    inline bool coord_valid_to_write_transition() {
        State expected = VALID;
        return st.compare_exchange_strong(expected, WRITE);                
    }

    inline void coord_write_to_valid_transition() {
        State expected = WRITE;
        st.compare_exchange_strong(expected, VALID);
    }
    
    inline void coord_write_to_invalid_transition() {
        State expected = WRITE;
        st.compare_exchange_strong(expected, INVALID);
    }

    inline void fol_invalid_to_replay_transition() {
        State expected = INVALID;
        st.compare_exchange_strong(expected, REPLAY);
    }

    inline void fol_replay_to_write_transition() {
        State expected = REPLAY;
        st.compare_exchange_strong(expected, WRITE);
    }

    // We check if the transition is possible before making it
    void fol_valid_to_invalid_transition(std::string value, HermesTimestamp ts) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        State expected = VALID;
        st.compare_exchange_strong(expected, INVALID);
        this->value = value;
        this->timestamp = Timestamp(ts);
    }

    // We check if the transition is possible before making it 
    inline void fol_invalid_to_valid_transition() {
        State expected = INVALID;
        st.compare_exchange_strong(expected, VALID);
    }
};
