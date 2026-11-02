#include "WorkerGroup.h"

#include <atomic>
#include <deque>
#include <iostream>
#include <stdexcept>

using namespace std::chrono_literals;
namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct State {
    std::atomic<int> prepared{0}, ran{0}, destroyed{0}, quiesced{0}, messages{0};
    std::atomic<bool> wrong_thread{false};
    std::mutex mutex;
    std::deque<int> queues[3];
    bool fail_prepare = false;
    bool fail_run = false;
};

class TestWorker final : public rkv::Worker {
public:
    TestWorker(State& state, std::size_t index) : state_(state), index_(index) {}
    ~TestWorker() override {
        if (owner_ != std::this_thread::get_id()) state_.wrong_thread = true;
        ++state_.destroyed;
    }
    void prepare() override {
        ++state_.prepared;
        if (state_.fail_prepare && index_ == 1) throw std::runtime_error("injected prepare failure");
    }
    void run() override {
        ++state_.ran;
        if (state_.fail_run && index_ == 1) throw std::runtime_error("injected run failure");
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [this] { return stopping_; });
    }
    void requestStop() noexcept override {
        std::lock_guard lock(mutex_);
        stopping_ = true;
        changed_.notify_all();
    }
    void quiesce() noexcept override { ++state_.quiesced; }
    bool drain() override {
        std::lock_guard lock(state_.mutex);
        require(state_.destroyed == 0, "worker destroyed before global drain completed");
        require(state_.quiesced == 3, "drain started before producers stopped");
        auto& queue = state_.queues[index_];
        if (queue.empty()) return false;
        int hops = queue.front();
        queue.pop_front();
        ++state_.messages;
        if (hops > 0) state_.queues[(index_ + 1) % 3].push_back(hops - 1);
        return true;
    }
private:
    State& state_;
    std::size_t index_;
    std::thread::id owner_ = std::this_thread::get_id();
    std::mutex mutex_;
    std::condition_variable changed_;
    bool stopping_ = false;
};

void testNormalStop() {
    State state;
    state.queues[0].push_back(30);
    rkv::WorkerGroup group;
    group.start(3, [&](std::size_t i) { return std::make_unique<TestWorker>(state, i); });
    require(state.prepared == 3, "start returned before preparation completed");
    group.requestStop();
    group.stop();
    group.stop();
    group.rethrowError();
    require(state.messages == 31, "cross-worker callbacks were dropped");
    require(state.destroyed == 3 && !state.wrong_thread, "wrong destruction ownership");
}

void testPrepareFailure() {
    State state;
    state.fail_prepare = true;
    rkv::WorkerGroup group;
    bool failed = false;
    try { group.start(3, [&](std::size_t i) { return std::make_unique<TestWorker>(state, i); }); }
    catch (const std::runtime_error& e) {
        failed = std::string(e.what()).find("worker 1 prepare") != std::string::npos;
    }
    require(failed && state.destroyed == 3 && state.ran == 0, "startup failure was not rolled back");
}

void testRuntimeFailure() {
    State state;
    state.fail_run = true;
    rkv::WorkerGroup group;
    bool failed = false;
    try {
        group.start(3, [&](std::size_t i) { return std::make_unique<TestWorker>(state, i); });
        require(group.waitForStop(2s), "runtime failure did not stop peers");
        group.stop();
        group.rethrowError();
    } catch (const std::runtime_error& e) {
        failed = std::string(e.what()).find("worker 1 run") != std::string::npos;
    }
    require(failed && state.destroyed == 3, "runtime error was not propagated/cleaned up");
}

void testFactoryFailure() {
    rkv::WorkerGroup group;
    bool failed = false;
    try {
        group.start(3, [](std::size_t) -> std::unique_ptr<rkv::Worker> {
            throw std::runtime_error("injected construction failure");
        });
    } catch (const std::runtime_error&) { failed = true; }
    require(failed, "construction failure was swallowed");
    group.stop();
}
}

int main() {
    try {
        for (int i = 0; i < 20; ++i) {
            testNormalStop();
            testPrepareFailure();
            testRuntimeFailure();
            testFactoryFailure();
        }
        std::cout << "lifecycle tests passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
