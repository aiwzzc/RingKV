#include "WorkerGroup.h"

#include <stdexcept>
#include <string>

namespace rkv {
WorkerGroup::~WorkerGroup() { stop(); }

void WorkerGroup::recordError(std::size_t index, const char* phase) noexcept {
    auto error = std::current_exception();
    try {
        try { std::rethrow_exception(error); }
        catch (const std::exception& e) {
            throw std::runtime_error("worker " + std::to_string(index) + " " + phase + ": " + e.what());
        }
    } catch (...) { error = std::current_exception(); }
    std::lock_guard lock(mutex_);
    if (!error_) error_ = error;
    stopLocked();
}

void WorkerGroup::start(std::size_t count, Factory factory) {
    if (started_) throw std::logic_error("WorkerGroup can only be started once");
    if (count == 0) throw std::invalid_argument("worker count must be positive");
    started_ = true;
    workers_.resize(count, nullptr);
    threads_.reserve(count);
    try {
        for (std::size_t i = 0; i < count; ++i)
            threads_.emplace_back([this, i, factory] { workerMain(i, factory); });
    } catch (...) {
        std::lock_guard lock(mutex_);
        if (!error_) error_ = std::current_exception();
        stopLocked();
    }
    {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [this] { return ready_ == threads_.size(); });
        released_ = true;
        changed_.notify_all();
    }
    try { rethrowError(); }
    catch (...) { stop(); throw; }
}

void WorkerGroup::workerMain(std::size_t index, const Factory& factory) noexcept {
    std::unique_ptr<Worker> worker;
    try {
        worker = factory(index);
        if (!worker) throw std::runtime_error("factory returned no worker");
        worker->prepare();
    } catch (...) { recordError(index, "prepare"); }
    bool run = false;
    {
        std::unique_lock lock(mutex_);
        workers_[index] = worker.get();
        ++ready_;
        changed_.notify_all();
        changed_.wait(lock, [this] { return released_; });
        run = !stopping_;
    }
    if (worker && run) {
        try { worker->run(); }
        catch (...) { recordError(index, "run"); }
        requestStop();
    }
    if (worker) worker->quiesce();

    std::unique_lock lock(mutex_);
    ++quiesced_;
    changed_.notify_all();
    std::size_t epoch = 0;
    while (true) {
        changed_.wait(lock, [this, epoch] { return destroy_ || drain_epoch_ != epoch; });
        if (destroy_) break;
        epoch = drain_epoch_;
        lock.unlock();
        bool worked = false;
        try { if (worker) worked = worker->drain(); }
        catch (...) { recordError(index, "drain"); worked = true; }
        lock.lock();
        did_work_ |= worked;
        ++drained_;
        changed_.notify_all();
    }
    workers_[index] = nullptr;
    lock.unlock();
    // Destruction stays on the owner thread, after global quiescence.
    worker.reset();
}

void WorkerGroup::stopLocked() noexcept {
    stopping_ = true;
    for (auto* worker : workers_) if (worker) worker->requestStop();
    changed_.notify_all();
}

void WorkerGroup::requestStop() noexcept {
    std::lock_guard lock(mutex_);
    stopLocked();
}

bool WorkerGroup::waitForStop(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout, [this] { return stopping_; });
}

void WorkerGroup::stop() noexcept {
    if (threads_.empty()) return;
    std::unique_lock lock(mutex_);
    stopLocked();
    changed_.wait(lock, [this] { return quiesced_ == threads_.size(); });
    // Repeated global rounds also drain work posted back by another worker.
    do {
        drained_ = 0;
        did_work_ = false;
        ++drain_epoch_;
        changed_.notify_all();
        changed_.wait(lock, [this] { return drained_ == threads_.size(); });
    } while (did_work_);
    destroy_ = true;
    changed_.notify_all();
    lock.unlock();
    for (auto& thread : threads_) if (thread.joinable()) thread.join();
    threads_.clear();
}

void WorkerGroup::rethrowError() const {
    std::lock_guard lock(mutex_);
    if (error_) std::rethrow_exception(error_);
}
}
