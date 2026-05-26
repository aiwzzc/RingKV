#include "kvserver.h"

#include "net/TcpConnection.h"
#include "persist/loader.h"

#include <latch>

namespace rkv {

kvserver::ExpireMap kvserver::expires_{};

kvserver::kvserver() : 
    engine_(&this->mempool_),
    protocol_(&this->engine_),
    context_(&this->mempool_, &this->engine_),
    Tcpserver_(&this->mempool_, Config::getInstance().port_)
     {}

void kvserver::start() {

    this->Tcpserver_.setMessageCallback([this] (const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& buf) {
        onMessage(conn, buf);
    });

    this->Tcpserver_.start();
}

void kvserver::onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& blocks) {

    if(Config::getInstance().cluster_enabled_ && Config::getInstance().is_master_ && 
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

RingKVServer::RingKVServer() = default;

RingKVServer::~RingKVServer() {
    for(auto& t : this->workers_) {
        if(t.joinable()) t.join();
    }
}

void RingKVServer::start() {

    Config& conf = Config::getInstance();
    conf.configParser();

    this->workers_size_ = conf.worker_threads_ >= 1 ? conf.worker_threads_ : 1;

    auto init_latch = std::make_shared<std::latch>(this->workers_size_ + 1);

    this->LoopsEngines_.resize(this->workers_size_);

    for(int i = 0; i < this->workers_size_; ++i) {
        this->workers_.emplace_back([this, i, init_latch] () {
            kvserver server{};

            this->LoopsEngines_[i] = std::make_pair<AeroIO::net::EventLoop*, Ringengine*>(
                server.getLoop(), 
                server.getEngine()
            );

            init_latch->arrive_and_wait();
            server.Tcpserver_.setLoopsEngines(&this->LoopsEngines_);
            server.Tcpserver_.getLoop()->setPersistFileIndex(i);
            server.LoaderManager_ = LoaderManager{server.getEngine(), &this->LoopsEngines_};
            server.LoaderManager_.start();

            server.start();
    
        });
    }

    init_latch->arrive_and_wait();
}

};