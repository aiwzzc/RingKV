#pragma once

#include <memory>
#include <thread>
#include <vector>
#include <deque>

#include "AeroIO/net/TcpServer.h"
#include "AeroIO/net/PendingWrite.h"
#include "AeroIO/net/http/HttpServer.h"
#include "protocolhandler.h"
#include "engine.h"
#include "jemalloc.h"
#include "persistloader.h"
#include "config.h"
#include "dict.h"

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
    AeroIO::net::EventLoop* getLoop() const;
    Ringengine* getEngine() const;
    Config* getConfig() const;

private:
    std::unique_ptr<JemallocWrapper> mempool_;
    std::unique_ptr<Ringengine> engine_;
    std::unique_ptr<KvsProtocolHandler> protocol_;
    std::unique_ptr<AeroIO::net::ReplyBufferPool> replyBufferPool_;
    std::unique_ptr<AeroIO::net::BlockPool> blockPool_;
    ServerContext context_;
    std::unique_ptr<TcpServer> Tcpserver_;
    std::unique_ptr<LoaderManager> LoaderManager_;

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