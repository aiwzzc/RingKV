#pragma once

#include <cstddef>
#include <sys/socket.h>

namespace AeroIO {

namespace net {

class Socket {

public:
    Socket(int sockfd);
    ~Socket();

    int fd() const;
    int getFixedIndex() const;
    void setNoregister();

    void bindAddress(int sockfd, const struct sockaddr* addr);
    void listen(int sockfd);

    static int setNoblockingSocket();
    void setFixedFileIndex(int);

    ///
    /// Enable/disable TCP_NODELAY (disable/enable Nagle's algorithm).
    ///
    void setTcpNoDelay(bool on);

    ///
    /// Enable/disable SO_REUSEADDR
    ///
    void setReuseAddr(bool on);

    ///
    /// Enable/disable SO_REUSEPORT
    ///
    void setReusePort(bool on);

    ///
    /// Enable/disable SO_KEEPALIVE
    ///
    void setKeepAlive(bool on);

private:
    int sockfd_;
    int fixed_file_index_;
    bool is_register_;

};

};

};