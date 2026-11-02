#pragma once

#include <condition_variable>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace rkv {

// Lifecycle seam: all methods except requestStop() run on the owning thread.
// quiesce() stops external producers; drain() runs queued work while every
// worker's resources are still alive. It returns true if any work was executed.
class Worker {
public:
    virtual ~Worker() = default;
    virtual void prepare() = 0;
    virtual void run() = 0;
    virtual void requestStop() noexcept = 0;
    virtual void quiesce() noexcept = 0;
    virtual bool drain() = 0;
};

class WorkerGroup {
public:
    using Factory = std::function<std::unique_ptr<Worker>(std::size_t)>;
    WorkerGroup() = default;
    ~WorkerGroup();
    WorkerGroup(const WorkerGroup&) = delete;
    WorkerGroup& operator=(const WorkerGroup&) = delete;

    // start()/stop() belong to the controller thread. One-shot lifecycle.
    void start(std::size_t count, Factory factory);
    void requestStop() noexcept;
    bool waitForStop(std::chrono::milliseconds timeout);
    void stop() noexcept;
    void rethrowError() const;

private:
    void workerMain(std::size_t index, const Factory& factory) noexcept;
    void stopLocked() noexcept;
    void recordError(std::size_t index, const char* phase) noexcept;

    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<std::thread> threads_;
    std::vector<Worker*> workers_;
    std::exception_ptr error_;
    bool started_ = false;
    bool released_ = false;
    bool stopping_ = false;
    bool destroy_ = false;
    std::size_t ready_ = 0;
    std::size_t quiesced_ = 0;
    std::size_t drain_epoch_ = 0;
    std::size_t drained_ = 0;
    bool did_work_ = false;
};
}
