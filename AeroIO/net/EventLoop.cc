#include "EventLoop.h"
#include "Acceptor.h"
#include "PendingWrite.h"
#include "TcpConnection.h"
#include "Log/LogApi.h"

#include <sys/eventfd.h>
#include <system_error>
#include <unistd.h>

namespace AeroIO::net {
namespace { thread_local EventLoop* current_loop = nullptr; }

EventLoop* EventLoop::getEventLoopOfCurrentThread() { return current_loop; }

EventLoop::EventLoop() :
    pending_functors_(std::make_unique<moodycamel::ConcurrentQueue<Functor>>()),
    fixed_fds_index_(1) {
    fixed_fds_.resize(FIXEDFDSIZE, -1);
    threadId_ = std::this_thread::get_id();
    auto ring = std::make_unique<io_uring>();
    io_uring_params params{};
    params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN |
                   IORING_SETUP_DEFER_TASKRUN;
    int result = io_uring_queue_init_params(kEntresLength, ring.get(), &params);
    if (result < 0)
        throw std::system_error(-result, std::generic_category(),
                               "io_uring setup (Linux 6.1+ with io_uring enabled required)");
    try {
        wakeupfd_ = createEventfd();
        result = io_uring_register_files(ring.get(), fixed_fds_.data(), fixed_fds_.size());
        if (result < 0) throw std::system_error(-result, std::generic_category(), "register files");
        result = io_uring_register_files_update(ring.get(), 0, &wakeupfd_, 1);
        if (result < 0) throw std::system_error(-result, std::generic_category(), "register wakeup fd");
        fixed_fds_[0] = wakeupfd_;
        io_uring_sync_cancel_reg cancel{};
        cancel.flags = IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL;
        cancel.timeout.tv_sec = -1;
        cancel.timeout.tv_nsec = -1;
        result = io_uring_register_sync_cancel(ring.get(), &cancel);
        if (result < 0 && result != -ENOENT)
            throw std::system_error(-result, std::generic_category(), "synchronous io_uring cancellation required");
    } catch (...) {
        io_uring_queue_exit(ring.get());
        if (wakeupfd_ >= 0) ::close(wakeupfd_);
        throw;
    }
    ring_ = ring.release();
    current_loop = this;
}

EventLoop::~EventLoop() {
    quiesceIo();
    io_uring_queue_exit(ring_);
    delete ring_;
    ::close(wakeupfd_);
    current_loop = nullptr;
}

void EventLoop::quiesceIo() noexcept {
    if (quiesced_) return;
    // The caller keeps connections, operation contexts and buffers alive here.
    int result;
    while (io_uring_sq_ready(ring_) != 0) {
        do { result = io_uring_submit(ring_); } while (result == -EINTR);
        if (result <= 0) std::terminate();
    }
    io_uring_sync_cancel_reg cancel{};
    cancel.flags = IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL;
    cancel.timeout.tv_sec = -1;
    cancel.timeout.tv_nsec = -1;
    do { result = io_uring_register_sync_cancel(ring_, &cancel); } while (result == -EINTR);
    // An unexpected kernel failure cannot justify freeing in-use buffers.
    if (result < 0 && result != -ENOENT) std::terminate();
    io_uring_cqe* completion;
    while (io_uring_peek_cqe(ring_, &completion) == 0) {
        auto data = io_uring_cqe_get_data64(completion);
        if (data != WAKEUP_MSG_DATA && data != 0) {
            auto* head = static_cast<GeneralHead*>(io_uring_cqe_get_data(completion));
            // A successful accept owns an fd even when its callback is discarded.
            if (head->type_ == IoType::ACCEPT && completion->res >= 0) ::close(completion->res);
        }
        io_uring_cqe_seen(ring_, completion);
    }
    quiesced_ = true;
}

bool EventLoop::drainTasks() { return doPendingFunctors(); }

io_uring* EventLoop::ring()
{ return this->ring_; }

std::vector<int>& EventLoop::getFixedFds()
{ return this->fixed_fds_; }

void EventLoop::queueInfreeFixedFds(int index)
{ this->free_fixed_fds_.push_back(index); }

int EventLoop::getFreeFixedFd() {
    if(this->free_fixed_fds_.empty()) return -1;

    int index = this->free_fixed_fds_.front();
    this->free_fixed_fds_.pop_front();

    return index;
}

int EventLoop::getNextIndex() {
    if (fixed_fds_index_ >= static_cast<int>(fixed_fds_.size())) return -1;
    return fixed_fds_index_++;
}

void EventLoop::addTimer(uint64_t ms, const TimerCallback& cb, TimerRequest* timer) {
    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    timer->setTimer(ms, cb);

    io_uring_prep_timeout(sqe, &timer->ts_, 0, 0);
    io_uring_sqe_set_data(sqe, timer);
}

bool EventLoop::isInLoopThread() const 
{ return std::this_thread::get_id() == this->threadId_; }

void EventLoop::runInLoop(Functor cb) {
    if(isInLoopThread()) {
        cb();
        return;
    }

    this->pending_functors_->enqueue(std::move(cb));

    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    if(this->is_sleeping_.load(std::memory_order_seq_cst)) {
        wakeup();
    }
}

int EventLoop::createEventfd() {
    int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evtfd < 0) {
        throw std::system_error(errno, std::generic_category(), "eventfd");
    }
    return evtfd;
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(this->wakeupfd_, &one, sizeof one);
    if (n < 0 && errno == EINTR) wakeup();
    // EAGAIN means eventfd is already readable; no additional wakeup is needed.
}

void EventLoop::armWakeupFd() {
    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_read(sqe, 0, &this->wakeup_buf_, sizeof(this->wakeup_buf_), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, (void*)WAKEUP_MSG_DATA);
    // std::cout << "post eventfd" << std::endl;
}

void EventLoop::prepare() { armWakeupFd(); }

void EventLoop::loop() {
    while (!quit_.load(std::memory_order_acquire)) {

        io_uring_cqe* cqe;
        unsigned count{0};

        while (io_uring_peek_cqe(ring_, &cqe) == 0) {
            const int result = cqe->res;
            const uint64_t user_data = io_uring_cqe_get_data64(cqe);
            auto* req_head = static_cast<GeneralHead*>(io_uring_cqe_get_data(cqe));
            // Consume before invoking user code: if a callback throws, shutdown
            // must not inspect an already-released operation a second time.
            io_uring_cqe_seen(ring_, cqe);

            if(user_data == WAKEUP_MSG_DATA) {
                this->armWakeupFd();
                ++count;

                continue;
            }

            if(req_head->type_== IoType::RECV) {
                IoRequest* recvreq = reinterpret_cast<IoRequest*>(req_head);
                recvreq->onComplete(result);

            } else if(req_head->type_ == IoType::SEND) {
                ReplyBuffer* replybuf = reinterpret_cast<ReplyBuffer*>(req_head);
                replybuf->onComplete(result);

            } else if(req_head->type_ == IoType::ACCEPT) {
                IoReqeustAcceptor* acceptorreq = reinterpret_cast<IoReqeustAcceptor*>(req_head);
                acceptorreq->onComplete(result);

            } else if(req_head->type_ == IoType::TIMER) {
                TimerRequest* timerreq = reinterpret_cast<TimerRequest*>(req_head);
                timerreq->onComplete(result);

            } else {
                LOG_FATAL("CRITICAL: Unknown CQE type");
            }

            ++count;
        };


        bool did_task = doPendingFunctors();
        if (quit_.load(std::memory_order_acquire)) break;
        if(count == 0 && !did_task) {
            this->is_sleeping_.store(true, std::memory_order_seq_cst);

            if(this->pending_functors_->size_approx() > 0) {
                this->is_sleeping_.store(false, std::memory_order_seq_cst);
                io_uring_submit(this->ring_);

            } else {
                int result = io_uring_submit_and_wait(this->ring_, 1);
                if (result < 0 && result != -EINTR)
                    throw std::system_error(-result, std::generic_category(), "io_uring wait");
                this->is_sleeping_.store(false, std::memory_order_seq_cst);
            }

        } else {
            io_uring_submit(this->ring_);
        }
    }
}

void EventLoop::quit() {
    if (quit_.exchange(true, std::memory_order_acq_rel)) return;

    if(!isInLoopThread()) {
        wakeup();
    }
}

bool EventLoop::doPendingFunctors() {
    bool is_did_task{false};
    Functor cb;

    while(this->pending_functors_->try_dequeue(cb)) 
    { cb(); is_did_task = true; }

    return is_did_task;
}

}
