#pragma once

#include <memory>

#include "net/TcpServer.h"

#include "protocolhandler.h"
#include "config.h"
#include "ShardRouter.h"
#include "RouteBatchTask.h"
#include "WorkerGroup.h"

#include "core/engine.h"

using AeroIO::net::TcpServer;

namespace AeroIO {
namespace net {

class TcpConnection;
class ReplyBufferPool;
class UringBuffer;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
class EventLoop;

} // namespace net
} // namespace AeroIO

namespace rkv {

class kvserver : public Worker {

public:
    kvserver(const Config& config, std::size_t index, ShardRouter& router);
    void prepare() override;
    void run() override;
    void requestStop() noexcept override;
    void quiesce() noexcept override;
    bool drain() override;
    void onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& buf);
    AeroIO::net::EventLoop* getLoop();
    Ringengine* getEngine();

private:
    const Config config_;
    JemallocWrapper mempool_{};
    RouteBatchTaskPool task_pool_;
    rdict engine_;
    KvsProtocolHandler protocol_;
    TcpServer Tcpserver_;
    std::size_t index_;
    ShardRouter& router_;

};

class RingKVServer {
public:
    explicit RingKVServer(Config config);
    void start();
    void requestStop() noexcept { workers_.requestStop(); }
    void stop() noexcept { workers_.stop(); }
    bool waitForStop(std::chrono::milliseconds timeout) { return workers_.waitForStop(timeout); }
    void rethrowError() const { workers_.rethrowError(); }

private:
    const Config config_;
    ShardRouter router_;
    // Last member: workers are stopped before router/configuration disappear.
    WorkerGroup workers_;
};
}
