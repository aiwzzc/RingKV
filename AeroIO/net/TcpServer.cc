#include "TcpServer.h"
#include "Socket.h"
#include "TcpConnection.h"
#include "PendingWrite.h"
#include "src/jemalloc.h"
#include "src/config.h"
#include "src/common.h"

#include <iostream>

namespace AeroIO {

namespace net {

TcpServer::TcpServer(rkv::ServerContext* ctx, int port, Option option) : Serverctx_(ctx), 
    loop_(std::make_unique<EventLoop>(ctx->engine)),
    acceptor_(std::make_unique<Acceptor>(this->loop_.get(), port, option == Option::kReusePort)) {

    this->acceptor_->setNewConnectionCallback([this, option] (int sockfd) {
        this->addNewConnection(sockfd);
    });

    RouteBatchTaskPool& taskPool = RouteBatchTaskPool::getInstance();
    taskPool.setMempool(this->Serverctx_->mempool);
    taskPool.initPool();
}

TcpConnectionPtr TcpServer::addNewConnection(int sockfd) {
    TcpConnectionPtr conn = std::make_shared<TcpConnection>(this->loop_.get(), sockfd, this->Serverctx_);

    int index = this->loop_->getFreeFixedFd();

    if(index < 0) {
        index = this->loop_->getNextIndex();
        if(index < 0) {
            // LOG_ERROR MAXCONNLIMIT
            return nullptr;
        }
    }

    this->loop_->getFixedFds()[index] = sockfd;
    
    io_uring_register_files_update(this->loop_->ring(), index, &sockfd, 1);

    conn->setFixedFileIndex(index);

    conn->setMessageCallback(this->messageCallback_);
    conn->setConnectionCallback(this->connectionCallback_);
    conn->setWriteCompleteCallback(this->writeCompleteCallback_);
    conn->setCloseCallback([this] (const TcpConnectionPtr& conn) {
        this->loop_->runInLoop([this, conn] () {
            if(conn->IsReplica()) this->loop_->removeFromReplicas_(conn);
            this->connections_.erase(conn->fd());
            conn->connectDestroyed();
        });
    });

    this->connections_[sockfd] = conn;

    this->loop_->runInLoop([conn] () {
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
    this->loop_->setTcpServer(this);
    this->loop_->setLoopsEngines(this->LoopsEngines_);
    this->acceptor_->start();
    this->loop_->loop();
}

EventLoop* TcpServer::getLoop() const
{ return this->loop_.get(); }

void TcpServer::setLoopsEngines(LoopsEngines LoopsEngines)
{ this->LoopsEngines_ = LoopsEngines; }

TcpServer::LoopsEngines TcpServer::getLoopsEngines() 
{ return this->LoopsEngines_; }

rkv::ServerContext* TcpServer::getServerContext() 
{ return this->Serverctx_; }

};

};