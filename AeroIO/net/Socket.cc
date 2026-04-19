#include "Socket.h"
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <iostream>
#include <thread>
#include <cstring>

namespace AeroIO {

namespace net {

Socket::Socket(int sockfd) : sockfd_(sockfd), 
fixed_file_index_(-1), is_register_(false) {};

Socket::~Socket() = default;

int Socket::fd() const
{ return this->sockfd_; }

int Socket::getFixedIndex() const {
    if(this->is_register_ && this->fixed_file_index_ >= 0) {
        return this->fixed_file_index_;
    }

    return -1;
}

void Socket::setNoregister()
{ this->is_register_ = false; }

int Socket::setNoblockingSocket() {
    int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (sockfd < 0) {
        // LOG_SYSFATAL << "sockets::createNonblockingOrDie";
    }

    return sockfd;
}

void Socket::setFixedFileIndex(int index) {
    if(this->is_register_) return;

    this->fixed_file_index_ = index;
    this->is_register_ = true;
}

void Socket::setTcpNoDelay(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY,
               &optval, static_cast<socklen_t>(sizeof optval));
}

void Socket::setReuseAddr(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR,
                &optval, static_cast<socklen_t>(sizeof optval));
}

void Socket::setReusePort(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT,
                &optval, static_cast<socklen_t>(sizeof optval));
}

void Socket::setKeepAlive(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE,
               &optval, static_cast<socklen_t>(sizeof optval));
}

void Socket::bindAddress(int sockfd, const struct sockaddr* addr) {
    int ret = ::bind(sockfd, addr, static_cast<socklen_t>(sizeof(struct sockaddr_in)));
    if(ret < 0) {
        if (ret < 0) {
            std::cerr << "[FATAL] Thread " << std::this_thread::get_id() 
                    << " bind failed! errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
            exit(EXIT_FAILURE); // 一旦绑定失败，直接让整个进程崩溃，不要让它变成僵尸死锁！
        }
    }
}

void Socket::listen(int sockfd) {
    int ret = ::listen(sockfd, SOMAXCONN);
    if(ret < 0) {
        // LOG_ERROR
    }
}

};

};