#include "TcpServer.h"
#include "EventLoop.h"
#include "TcpConnection.h"
#include "PendingWrite.h"

#include "Log/LogApi.h"

// #include "src/config.h"
// #include "src/common.h"

namespace AeroIO {

namespace net {

TcpServer::TcpServer(rkv::JemallocWrapper* mempool, int port, Option option) : 
    mempool_(mempool), 
    acceptor_(&this->loop_, port, option == Option::kReusePort),
    replyBufferPool_(mempool, 10), 
    blockPool_(mempool) {

    this->acceptor_.setNewConnectionCallback([this, option] (int sockfd) {
        this->addNewConnection(sockfd);
    });

    // RouteBatchTaskPool& taskPool = RouteBatchTaskPool::getInstance();
    // taskPool.setMempool(mempool);
    // taskPool.initPool();
}

TcpConnectionPtr TcpServer::addNewConnection(int sockfd) {
    PoolContext poolctx{&this->replyBufferPool_, &this->blockPool_};
    TcpConnectionPtr conn = std::make_shared<TcpConnection>(&this->loop_, sockfd, poolctx);

    int index = this->loop_.getFreeFixedFd();

    if(index < 0) {
        index = this->loop_.getNextIndex();
        if(index < 0) {
            LOG_ERROR("MAXCONNLIMIT");
            return nullptr;
        }
    }

    this->loop_.getFixedFds()[index] = sockfd;
    
    io_uring_register_files_update(this->loop_.ring(), index, &sockfd, 1);

    conn->setFixedFileIndex(index);

    conn->setMessageCallback(this->messageCallback_);
    conn->setConnectionCallback(this->connectionCallback_);
    conn->setWriteCompleteCallback(this->writeCompleteCallback_);
    conn->setCloseCallback([this] (const TcpConnectionPtr& conn) {
        this->loop_.runInLoop([this, conn] () {
            if(conn->IsReplica()) this->loop_.removeFromReplicas_(conn);
            this->connections_.erase(conn->fd());
            conn->connectDestroyed();
        });
    });

    this->connections_[sockfd] = conn;

    this->loop_.runInLoop([conn] () {
        conn->connectEstablished();
        conn->start();
    });

    return conn;
}

void TcpServer::removeConnection(int sockfd) {
    auto it = this->connections_.find(sockfd);

    if(it != this->connections_.end()) {
        this->connections_.erase(it);
    }
}

void TcpServer::addConnections(int sockfd, TcpConnectionPtr conn) {
    auto it = this->connections_.find(sockfd);

    if(it == this->connections_.end()) {
        this->connections_[sockfd] = conn;
    }
}

void TcpServer::setMessageCallback(const MessageCallback& cb)
{ this->messageCallback_ = cb; }

void TcpServer::setConnectionCallback(const ConnectionCallback& cb)
{ this->connectionCallback_ = cb; }

void TcpServer::setWriteCompleteCallback(const WriteCompleteCallback& cb)
{ this->writeCompleteCallback_ = cb; }

void TcpServer::start() {
    this->loop_.setTcpServer(this);
    this->loop_.setLoopsEngines(this->LoopsEngines_);
    this->acceptor_.start();
    this->loop_.loop();
}

EventLoop* TcpServer::getLoop()
{ return &this->loop_; }

void TcpServer::setLoopsEngines(LoopsEngines LoopsEngines)
{ this->LoopsEngines_ = LoopsEngines; }

TcpServer::LoopsEngines TcpServer::getLoopsEngines() 
{ return this->LoopsEngines_; }

};

};