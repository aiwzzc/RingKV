#include "kvserver.h"
#include "RouteBatchTask.h"

#include "config.h"
#include "net/TcpConnection.h"


namespace rkv {
namespace {
Config checkedConfig(Config config) {
    config.validate();
    return config;
}
}


kvserver::kvserver(const Config& config, std::size_t index, ShardRouter& router) :
    config_(config), task_pool_(mempool_), engine_(&mempool_),
    protocol_(engine_, task_pool_, router),
    Tcpserver_(&mempool_, config.port_), index_(index), router_(router) {}

void kvserver::prepare() {
    router_.bind(index_, *getLoop(), engine_);
    Tcpserver_.setMessageCallback([this](const auto& connection, auto& buffers) {
        onMessage(connection, buffers);
    });
    Tcpserver_.prepare(config_.ip_);
}

void kvserver::run() { Tcpserver_.start(); }
void kvserver::requestStop() noexcept { getLoop()->quit(); }
void kvserver::quiesce() noexcept { Tcpserver_.quiesce(); }
bool kvserver::drain() { return getLoop()->drainTasks(); }

void kvserver::onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& blocks) {

    this->protocol_.handleProto(conn, blocks);
}

AeroIO::net::EventLoop* kvserver::getLoop()
{ return this->Tcpserver_.getLoop(); }

Ringengine* kvserver::getEngine()
{ return &this->engine_; }

RingKVServer::RingKVServer(Config config) : config_(checkedConfig(std::move(config))), router_(config_.shard_threads_) {}

void RingKVServer::start() {
    workers_.start(config_.shard_threads_, [this](std::size_t index) {
        return std::make_unique<kvserver>(config_, index, router_);
    });
}
}
