#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <functional>

template <typename Key, typename Value>
class ThreadSafeUnorderedMap {
private:
    std::unordered_map<Key, Value> _map; // The underlying unordered_map
    mutable std::shared_mutex _mutex;   // To synchronize access

public:
    ThreadSafeUnorderedMap() = default;

    ~ThreadSafeUnorderedMap() = default;

    // Insert or update a value
    void insertOrUpdate(const Key& key, const Value& value) {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _map[key] = value;
    }

    // Retrieve a value by key
    std::optional<Value> get(const Key& key) const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto it = _map.find(key);
        if (it != _map.end()) {
            return it->second;
        }
        return std::nullopt; // Key not found
    }

    // Remove a key-value pair
    void erase(const Key& key) {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _map.erase(key);
    }

    // Check if a key exists
    bool contains(const Key& key) const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        return _map.find(key) != _map.end();
    }

    // Get the size of the map
    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        return _map.size();
    }

    // Clear the map
    void clear() {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        _map.clear();
    }

    // Iterate with a callback
    void forEach(const std::function<void(const Key&, const Value&)>& callback) const {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        for (const auto& [key, value] : _map) {
            callback(key, value);
        }
    }
};
