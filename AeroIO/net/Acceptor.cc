#include "Acceptor.h"
#include "EventLoop.h"

#include <stdexcept>

namespace AeroIO {

namespace net {

Acceptor::Acceptor(EventLoop* loop, int port, bool reusePort) :
    loop_(loop), acceptSocket_(Socket::setNoblockingSocket()), port_(port) {
    acceptSocket_.setReusePort(reusePort);
}

void Acceptor::prepare(const std::string& ip) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    if (::inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1) {
        throw std::invalid_argument("invalid IPv4 listen address");
    }

    acceptSocket_.bindAddress(acceptSocket_.fd(), reinterpret_cast<sockaddr*>(&address));
    acceptSocket_.listen(acceptSocket_.fd());

    auto addr = std::make_unique<sockaddr_in>();
    accept_req_ = new IoReqeustAcceptor(this, reinterpret_cast<sockaddr*>(addr.release()));
    accept_req_->head_.type_ = IoType::ACCEPT;
    accept_req_->addrlen_ = sizeof(sockaddr_in);
}

Acceptor::~Acceptor() {
    if (accept_req_) {
        delete reinterpret_cast<sockaddr_in*>(accept_req_->addr_);
        delete accept_req_;
    }
}

void Acceptor::setNewConnectionCallback(const NewConnectionCallback& cb)
{ this->newConnectionCallback_ = cb; }

void Acceptor::start() {
    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());
        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    io_uring_prep_accept(sqe, this->acceptSocket_.fd(), this->accept_req_->addr_, &this->accept_req_->addrlen_, 0);
    io_uring_sqe_set_data(sqe, this->accept_req_);

}

int Acceptor::acceptorfd() const
{ return this->acceptSocket_.fd(); }

void Acceptor::handleRead(int res_bytes) {
    if(res_bytes >= 0) {
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