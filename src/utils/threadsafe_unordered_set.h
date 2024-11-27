#include <unordered_set>
#include <mutex>
#include <shared_mutex>

template <typename T>
class ThreadSafeUnorderedSet {
private:
    std::unordered_set<T> _set;
    mutable std::shared_mutex _mutex;
public:
    ThreadSafeUnorderedSet() {}; // = default;

    explicit ThreadSafeUnorderedSet(std::unordered_set<T>& init_set) {
        _set = init_set;
    }

    // Copy constructor
    ThreadSafeUnorderedSet(const ThreadSafeUnorderedSet& other) {
        std::shared_lock<std::shared_mutex> lock(other._mutex);
        _set = other._set;
    }

    // Copy assignment operator
    ThreadSafeUnorderedSet& operator=(const ThreadSafeUnorderedSet& other) {
        if (this != &other) { // Check for self-assignment
            std::unique_lock<std::shared_mutex> lock_this(_mutex); // Lock current object
            std::shared_lock<std::shared_mutex> lock_other(other._mutex); // Lock the other object
            _set = other._set; // Copy the data
        }
        return *this;
    }

    ~ThreadSafeUnorderedSet() = default;

    std::unordered_set<T> copy() const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        return _set;
    }

    void insert(const T& value) {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _set.insert(value);
    }

    void erase(const T& value) {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _set.erase(value);
    }

    size_t size() const {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        return _set.size();
    }

    bool contains(const T& value) const {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        return (_set.find(value) != _set.end());
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _set.clear();
    }

    bool empty() const {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        return _set.empty();
    }

    //void print() {
    //    std::unique_lock<std::shared_mutex> lock(_mutex);
    //    
    //}

};