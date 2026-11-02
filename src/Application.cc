#include "Application.h"
#include "config.h"
#include "kvserver.h"
#include "Log/LoggerManager.h"

#include <csignal>
#include <iostream>
#include <pthread.h>
#include <system_error>

namespace rkv {
namespace {
// Block before creating any thread. The controller consumes signals synchronously;
// no mutexes, allocations, logging or C++ destructors run in a signal handler.
class TerminationSignals {
public:
    TerminationSignals() {
        sigemptyset(&signals_);
        sigaddset(&signals_, SIGINT);
        sigaddset(&signals_, SIGTERM);
        int error = pthread_sigmask(SIG_BLOCK, &signals_, &previous_);
        if (error) throw std::system_error(error, std::generic_category(), "block termination signals");
        struct sigaction ignore{};
        ignore.sa_handler = SIG_IGN;
        sigemptyset(&ignore.sa_mask);
        // writev on a peer-reset socket must report an I/O error, not kill the process.
        if (sigaction(SIGPIPE, &ignore, &previous_pipe_) < 0) {
            const int error = errno;
            pthread_sigmask(SIG_SETMASK, &previous_, nullptr);
            throw std::system_error(error, std::generic_category(), "ignore SIGPIPE");
        }
    }
    ~TerminationSignals() {
        sigaction(SIGPIPE, &previous_pipe_, nullptr);
        pthread_sigmask(SIG_SETMASK, &previous_, nullptr);
    }
    bool received() {
        timespec timeout{0, 100'000'000};
        int signal = sigtimedwait(&signals_, nullptr, &timeout);
        if (signal == SIGINT || signal == SIGTERM) return true;
        if (signal < 0 && errno != EAGAIN && errno != EINTR)
            throw std::system_error(errno, std::generic_category(), "wait for termination signal");
        return false;
    }
private:
    sigset_t signals_{};
    sigset_t previous_{};
    struct sigaction previous_pipe_{};
};

class LoggingLifetime {
public:
    LoggingLifetime() { AeroIO::Logger::LoggerManager::start(); }
    ~LoggingLifetime() { AeroIO::Logger::LoggerManager::stop(); }
};
}

int Application::run(int argc, char* argv[]) {
    try {
        ConfigBuilder builder(argc, argv);
        if (builder.help()) {
            std::cout << "Usage: RingKV [--config FILE] [--ip IPv4] [--port PORT] [--threads N]\n"
                         "              [--check-config] [--dump-config] [--help]\n"
                         "Precedence: defaults < config file < command line.\n";
            return 0;
        }
        const Config config = builder.Build();
        if (builder.dumpOnly()) { std::cout << config.dump(); return 0; }
        if (builder.checkOnly()) { std::cout << "Configuration OK\n"; return 0; }

        TerminationSignals signals;
        LoggingLifetime logging;
        RingKVServer server(config);
        server.start();
        std::cout << "RingKV ready on " << config.ip_ << ':' << config.port_
                  << " (" << config.shard_threads_ << " workers)" << std::endl;
        while (!server.waitForStop(std::chrono::milliseconds(0))) {
            if (signals.received()) break;
        }
        server.stop();
        server.rethrowError();
        std::cout << "RingKV stopped" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "RingKV: " << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "RingKV: unknown fatal error\n";
        return 1;
    }
}
}
