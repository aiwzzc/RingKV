#include "Acceptor.h"
#include "EventLoop.h"

#include <string.h>
#include <iostream>

namespace AeroIO {

namespace net {

Acceptor::Acceptor(EventLoop* loop, int port, bool ReUsePort) : 
    loop_(loop), acceptSocket_(Socket::setNoblockingSocket()) {

    sockaddr_in server_addr;
    ::memset(&server_addr, 0, sizeof(sockaddr_in));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    this->acceptSocket_.setReusePort(true);

    this->acceptSocket_.bindAddress(this->acceptSocket_.fd(), (sockaddr*)&server_addr);
    this->acceptSocket_.listen(this->acceptSocket_.fd());
}

Acceptor::~Acceptor() {
    ::close(this->acceptSocket_.fd());
    delete this->accept_req_->addr_;
    delete this->accept_req_;
}

void Acceptor::setNewConnectionCallback(const NewConnectionCallback& cb)
{ this->newConnectionCallback_ = cb; }

void Acceptor::start() {
    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());
        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    sockaddr_in* addr = new sockaddr_in;
    this->accept_req_ = new IoReqeustAcceptor(this, (sockaddr*)addr);
    this->accept_req_->head_.type_ = IoType::ACCEPT;
    this->accept_req_->addrlen_ = sizeof(sockaddr_in);

    io_uring_prep_accept(sqe, this->acceptSocket_.fd(), (sockaddr*)addr, &this->accept_req_->addrlen_, 0);
    io_uring_sqe_set_data(sqe, this->accept_req_);

}

int Acceptor::acceptorfd() const
{ return this->acceptSocket_.fd(); }

void Acceptor::handleRead(int res_bytes) {
    if(res_bytes > 0) {
        if(this->newConnectionCallback_) {
            this->newConnectionCallback_(res_bytes);
        }

    } else if (res_bytes < 0 && res_bytes != -EAGAIN) {

    }

    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());
        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    IoReqeustAcceptor* req = this->accept_req_;
    req->addrlen_ = sizeof(sockaddr_in);

    io_uring_prep_accept(sqe, this->acceptSocket_.fd(), req->addr_, &req->addrlen_, 0);
    io_uring_sqe_set_data(sqe, req);
}

IoReqeustAcceptor::IoReqeustAcceptor(Acceptor* acceptor, sockaddr* addr) : 
    acceptor_(acceptor), addr_(addr) {}

void IoReqeustAcceptor::onComplete(int res_bytes) {

    if(this->acceptor_ && this->head_.type_ == IoType::ACCEPT) {
        this->acceptor_->handleRead(res_bytes);
    }
}

};

};