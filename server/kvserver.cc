#include "kvserver.h"
#include "RouteBatchTask.h"

#include "config.h"
#include "net/TcpConnection.h"
#include "persist/loader.h"


namespace rkv {

kvserver::kvserver(const Config& config, std::size_t index, Topology& topology) :
    config_(config), task_pool_(mempool_), engine_(&mempool_),
    protocol_(&engine_, task_pool_), context_(&mempool_, &engine_),
    Tcpserver_(&mempool_, config.port_), index_(index), topology_(topology) {}

void kvserver::prepare() {
    topology_[index_] = {getLoop(), getEngine()};
    Tcpserver_.setLoopsEngines(&topology_);
    getLoop()->setPersistFileIndex(static_cast<int>(index_));
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

    if(this->config_.cluster_enabled_ && this->config_.is_master_ && 
        conn->getConnState() == AeroIO::net::ConnState::HANDSHAKING) {

        const char* crlf = nullptr;
        AeroIO::net::BlockPtr buf;

        for(auto& block : blocks) {
            if((crlf = block->findCRLF())) {
                buf = block;
                break;
            }
        }
            
        if(crlf == nullptr) return;

        std::string shakeStr(buf->peek(), crlf - buf->peek());

        if(shakeStr.find("SYNC_HANDSHAKE-") == 0) {
            int target_thread_index = std::stoi(shakeStr.substr(15));
            AeroIO::net::EventLoop* curr_loop = conn->getLoop();

            buf->retrieve(shakeStr.size() + 2);
            conn->setConnState(AeroIO::net::ConnState::REPLICA);

            if(target_thread_index == conn->getLoop()->getConnIndex()) {
                conn->getLoop()->startFullSync(conn);
                conn->getLoop()->addToReplicas_(conn);
                conn->setIsReplica(true);

            } else {

                if(int conn_fixed_index = conn->getFixedIndex(); conn_fixed_index >= 0) {
                    auto new_conn = conn;

                    curr_loop->queueInfreeFixedFds(conn_fixed_index);
                    curr_loop->getFixedFds()[conn_fixed_index] = -1;
                    int fd_to_remove = -1;
                    io_uring_register_files_update(curr_loop->ring(), conn_fixed_index, &fd_to_remove, 1);
                    this->Tcpserver_.removeConnection(conn->fd());

                    conn->detachFromLoop();

                    AeroIO::net::EventLoop* target_loop = (*this->Tcpserver_.getLoopsEngines())[target_thread_index].first;
                    target_loop->runInLoop([target_loop, new_conn] () {
                        new_conn->attachToLoop(target_loop);

                        target_loop->startFullSync(new_conn);
                        target_loop->addToReplicas_(new_conn);
                        new_conn->setIsReplica(true);
                    });
                }
            }

            return;

        } else {
            conn->setConnState(AeroIO::net::ConnState::NORMAL_CLIENT);
        }
    }

    this->protocol_.handleProto(conn, blocks);
}

AeroIO::net::EventLoop* kvserver::getLoop()
{ return this->Tcpserver_.getLoop(); }

Ringengine* kvserver::getEngine()
{ return &this->engine_; }

RingKVServer::RingKVServer(Config config) : config_(std::move(config)) {
    config_.validate();
    topology_.resize(config_.shard_threads_);
}

void RingKVServer::start() {
    workers_.start(config_.shard_threads_, [this](std::size_t index) {
        return std::make_unique<kvserver>(config_, index, topology_);
    });
}
}
