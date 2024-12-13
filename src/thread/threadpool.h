#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <functional>

using Task = std::function<void()>;

class Threadpool {
private:
    int _num_threads;
    std::vector<std::thread> _threads;
    std::queue<Task> _tasks; // queue of tasks not assigned to any thread
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _stop_requested;

public:
    Threadpool() = default;

    Threadpool(int num_threads) :
        _num_threads(num_threads),
        _stop_requested(false)
        {}

    ~Threadpool() {}

    void addTask(Task task) {}
    void processingLoop() {}
    void start() {}
    void stop() {}

    bool isStopRequested(){
        return _stop_requested;
    }
};