#pragma once

#include "Socket.h"
#include "Callbacks.h"
#include "IoRequest.h"

#include <liburing.h>

namespace AeroIO {

namespace net {

class EventLoop;
class IoReqeustAcceptor;

class Acceptor {

public:
    Acceptor(EventLoop* loop, int port, bool ReUsePort);
    ~Acceptor();

    void start();

    void setNewConnectionCallback(const NewConnectionCallback&);

    void handleRead(int res_bytes);
    int acceptorfd() const;

private:

    EventLoop* loop_;
    Socket acceptSocket_;
    IoReqeustAcceptor* accept_req_;

    NewConnectionCallback newConnectionCallback_;
};

struct IoReqeustAcceptor {
    GeneralHead head_;

    Acceptor* acceptor_;
    sockaddr* addr_;
    socklen_t addrlen_;

    IoReqeustAcceptor(Acceptor* acceptor, sockaddr* addr);
    void onComplete(int res_bytes);
};

};

};