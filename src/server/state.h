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

    // std::string to_string() {
    //     std::string res = "Logical time: " + 
    // }

    Timestamp(HermesTimestamp ts) {
        this->logical_time = ts.local_ts();
        this->node_id = ts.node_id();
    }

    std::string toString() {
        std::ostringstream stream;
        stream << node_id << "." << logical_time;
        return stream.str();
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

    HermesValue(const std::string &key, const std::string &value, uint32_t node_id) {
        this->key = key;
        this->value = value;
        timestamp.node_id = node_id;
        timestamp.logical_time = 0;
        st.store(VALID, std::memory_order_release);
    }
    
    // Helper function that waits till the condition variable is notified
    void wait_till_valid() {
        std::unique_lock<std::mutex> lock(stall_mutex);

        // Wait till the key is in VALID state
        stall_cv.wait(lock, [this] {return is_valid();});
    }

    // Helper function that waits till the condition variable is notified with an additional timeout
    // Timeout is in seconds
    bool wait_till_valid_or_timeout(uint32_t timeout) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        
        // Wait till the key is in VALID state or timeout occurs
        // returns the latest value of is_valid(). If the wait completes due to key transitioning to valid 
        // then the return value will be true else false
        return stall_cv.wait_for(lock, std::chrono::seconds(timeout), [this] {return is_valid();});
        //// Return true if no_timeout. This is used to determine whether to stall client request
        //return status == std::cv_status::no_timeout;
    }

    inline void coord_valid_to_write_transition(const std::string &new_value, uint32_t node_id) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        State expected = VALID;
        st.compare_exchange_strong(expected, WRITE);
        timestamp.logical_time++;
        timestamp.node_id = node_id;
        value = new_value;
        //return timestamp;
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

    inline bool is_write() {
        return (st.load(std::memory_order_acquire) == WRITE);
    }

    inline void coord_write_to_valid_transition() {
        State expected = WRITE;
        st.compare_exchange_strong(expected, VALID);
        stall_cv.notify_one();
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
    void fol_invalidate(std::string value, HermesTimestamp ts) {
        std::unique_lock<std::mutex> lock(stall_mutex);
        st.store(INVALID, std::memory_order_release);
        this->value = value;
        this->timestamp = Timestamp(ts);
    }

    // We check if the transition is possible before making it 
    inline void fol_invalid_to_valid_transition() {
        State expected = INVALID;
        st.compare_exchange_strong(expected, VALID);
        stall_cv.notify_one();
    }

    inline Timestamp getTimestamp() {
        return timestamp;
    }

    inline State getState() {
        return st.load();
    }
};
