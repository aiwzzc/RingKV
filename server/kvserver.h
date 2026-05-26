#pragma once

#include <memory>
#include <thread>
#include <vector>

#include "net/TcpServer.h"

#include "protocolhandler.h"
#include "config.h"
#include "ServerContext.h"

#include "core/engine.h"
#include "base/jemalloc.h"
#include "persist/loader.h"
#include "ds/dict.h"

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

constexpr std::size_t REPLYBUFFERPOOLSIZE = 4096;

class kvserver {

public:
    using ExpireMap = rhash_sec<std::string, uint64_t>;
    static ExpireMap expires_;

    friend class RingKVServer;

    kvserver();

    void start();
    void onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& buf);
    AeroIO::net::EventLoop* getLoop();
    Ringengine* getEngine();
    Config* getConfig() const;

private:
    JemallocWrapper mempool_{};
    rdict engine_;
    KvsProtocolHandler protocol_;
    ServerContext context_;
    TcpServer Tcpserver_;
    LoaderManager LoaderManager_{nullptr, nullptr};

    bool is_Shaking_;

};

class RingKVServer {

public:
    RingKVServer();
    ~RingKVServer();

    void start();

private:
    std::vector<std::pair<AeroIO::net::EventLoop*, Ringengine*>> LoopsEngines_;
    std::vector<std::thread> workers_;
    // std::unique_ptr<HttpServer> httpServer_;
    
    std::size_t workers_size_;

};

};