#include "TcpServer.h"
#include "EventLoop.h"
#include "TcpConnection.h"
#include "PendingWrite.h"

#include "Log/LogApi.h"
#include <system_error>

namespace AeroIO {

namespace net {

TcpServer::TcpServer(base::JemallocWrapper* mempool, int port, AcceptorOption option) :
    mempool_(mempool), 
    acceptor_(&this->loop_, port, option == AcceptorOption::kReusePort),
    replyBufferPool_(mempool, 10), 
    blockPool_(mempool) {

    this->acceptor_.setNewConnectionCallback([this, option] (int sockfd) {
        this->addNewConnection(sockfd);
    });
}

TcpConnectionPtr TcpServer::addNewConnection(int sockfd) {
    PoolContext poolctx{&this->replyBufferPool_, &this->blockPool_};
    TcpConnectionPtr conn = std::make_shared<TcpConnection>(&this->loop_, sockfd, poolctx);

    int index = this->loop_.getFreeFixedFd();

    if(index < 0) {
        index = this->loop_.getNextIndex();
        if(index < 0) {
            LOG_ERROR("MAXCONNLIMIT");
            conn->stopAfterIoCancelled();
            return nullptr;
        }
    }

    this->loop_.getFixedFds()[index] = sockfd;
    
    int registered = io_uring_register_files_update(this->loop_.ring(), index, &sockfd, 1);
    if (registered < 0) {
        loop_.getFixedFds()[index] = -1;
        loop_.queueInfreeFixedFds(index);
        conn->stopAfterIoCancelled();
        throw std::system_error(-registered, std::generic_category(), "register connection fd");
    }

    conn->setFixedFileIndex(index);

    conn->setMessageCallback(this->messageCallback_);
    conn->setConnectionCallback(this->connectionCallback_);
    conn->setWriteCompleteCallback(this->writeCompleteCallback_);
    conn->setCloseCallback([this] (const TcpConnectionPtr& conn) {
        this->loop_.runInLoop([this, conn] () {
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

TcpServer::~TcpServer() { quiesce(); }

void TcpServer::prepare(const std::string& ip) {
    acceptor_.prepare(ip);
    loop_.prepare();
}

void TcpServer::start() {
    acceptor_.start();
    loop_.loop();
}

void TcpServer::quiesce() noexcept {
    loop_.quiesceIo();
    for (auto& [fd, conn] : connections_) conn->stopAfterIoCancelled();
    connections_.clear();
}

EventLoop* TcpServer::getLoop()
{ return &this->loop_; }

}
}
