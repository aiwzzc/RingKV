#include "Socket.h"
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <cstring>
#include <unistd.h>
#include <system_error>

namespace AeroIO {

namespace net {

Socket::Socket(int sockfd) : sockfd_(sockfd), 
fixed_file_index_(-1), is_register_(false) {};

Socket::~Socket() { close(); }

void Socket::close() noexcept {
    if (sockfd_ >= 0) { ::close(sockfd_); sockfd_ = -1; }
}

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
    if (sockfd < 0) throw std::system_error(errno, std::generic_category(), "socket");

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
    if (::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof optval) < 0)
        throw std::system_error(errno, std::generic_category(), "SO_REUSEPORT");
}

void Socket::setKeepAlive(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE,
               &optval, static_cast<socklen_t>(sizeof optval));
}

void Socket::bindAddress(int sockfd, const struct sockaddr* addr) {
    int ret = ::bind(sockfd, addr, static_cast<socklen_t>(sizeof(struct sockaddr_in)));
    if (ret < 0) throw std::system_error(errno, std::generic_category(), "bind");
}

void Socket::listen(int sockfd) {
    int ret = ::listen(sockfd, SOMAXCONN);
    if (ret < 0) throw std::system_error(errno, std::generic_category(), "listen");
}

};

};