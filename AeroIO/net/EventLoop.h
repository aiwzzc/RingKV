#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <liburing.h>

#include "IoRequest.h"
#include "ConcurrentQueue.h"

namespace AeroIO::net {
inline constexpr uint64_t WAKEUP_MSG_DATA = 0xFFFFFFFFFFFFFFFF;
inline constexpr std::size_t kEntresLength = 8192;
inline constexpr std::size_t FIXEDFDSIZE = 2000;

class EventLoop {
public:
    using Functor = std::function<void()>;
    using TimerCallback = std::function<void()>;

    EventLoop();
    ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    static EventLoop* getEventLoopOfCurrentThread();
    bool isInLoopThread() const;
    void runInLoop(Functor cb);
    void prepare();
    void loop();
    void quiesceIo() noexcept;
    bool drainTasks();
    void quit();
    io_uring* ring();
    void addTimer(uint64_t ms, const TimerCallback& cb, TimerRequest* timer);

    void queueInfreeFixedFds(int index);
    int getFreeFixedFd();
    int getNextIndex();
    std::vector<int>& getFixedFds();

private:
    int createEventfd();
    void wakeup();
    bool doPendingFunctors();
    void armWakeupFd();

    io_uring* ring_ = nullptr;
    std::atomic<bool> quit_{false};
    bool quiesced_ = false;
    std::thread::id threadId_;
    int wakeupfd_ = -1;
    uint64_t wakeup_buf_ = 0;
    std::vector<int> fixed_fds_;
    std::deque<int> free_fixed_fds_;
    int fixed_fds_index_;
    std::unique_ptr<moodycamel::ConcurrentQueue<Functor>> pending_functors_;
    std::atomic<bool> is_sleeping_{false};
};
}
