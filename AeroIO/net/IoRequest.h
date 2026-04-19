#pragma once

#include <memory>
#include <sys/uio.h>
#include <functional>
#include <linux/time_types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "Buffer.h"

namespace AeroIO {

namespace net {

enum class IoType { 
    ACCEPT, RECV, SEND, HANDSHAKE,
    CLOSE, AOF_WRITE, AOF_FSYNC, 
    RDB_WRITE, TIMER, TIMEOUT, AOF_REWRITE,
    REPL_SEND, HTTP_CONNECT, HTTP_SEND
};

constexpr std::size_t MAXIOVSIZE = 512;

class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
class EventLoop;

struct GeneralHead {
    IoType type_;
};

struct IoRequest {

    GeneralHead head_;
    std::weak_ptr<TcpConnection> conn_;

    IoRequest(TcpConnectionPtr conn);
    ~IoRequest();

    void setConn(TcpConnectionPtr conn);
    void setType(IoType type);
    void onComplete(int res_bytes);
};

struct CloseRequest {

    GeneralHead head_;
    std::weak_ptr<TcpConnection> conn_;

    CloseRequest(TcpConnectionPtr conn);
    ~CloseRequest();

    void onComplete();
    void setType(IoType type);
};

struct AofRequest {

    GeneralHead head_;
    EventLoop* loop_;

    AofRequest();
    ~AofRequest();

    void onComplete(int res_bytes);
    void setType(IoType type);
    void setLoop(EventLoop* loop);

};

struct AoffsyncRequest {

    GeneralHead head_;
    EventLoop* loop_;

    void setType(IoType type);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);

};

struct AofRewriteRequest {

    GeneralHead head_;
    EventLoop* loop_;

    void setType(IoType type);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);

};

struct RdbWriteRequest {

    GeneralHead head_;
    EventLoop* loop_;

    void setType(IoType type);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);

};

struct TimerRequest {

    GeneralHead head_;
    EventLoop* loop_;

    struct __kernel_timespec ts_;
    std::function<void()> callback_;

    TimerRequest();
    void setLoop(EventLoop* loop);
    void setType(IoType type);
    void setTimer(uint64_t ms, std::function<void()> cb);
    void onComplete(int res_bytes);
};

enum class HandshakeState {
    CONNECTING,
    WRITING,
    DONE
};

struct HandShakeRequest {

    GeneralHead head_;
    EventLoop* loop_;
    sockaddr_in addr_;
    int fd_;
    int error_count_;
    std::size_t write_offset_;
    HandshakeState state_{HandshakeState::CONNECTING};

    ~HandShakeRequest();

    void setType(IoType type);
    void setFd(int fd);
    void setAddr(const char* ip, int port);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);
    void reset();

};

struct SendReplicaRequest {

    GeneralHead head_;
    EventLoop* loop_;
    TcpConnectionPtr conn_;

    void setType(IoType type);
    void setConn(const TcpConnectionPtr& conn);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);

};

struct HttpConnectRequest {

    GeneralHead head_;
    EventLoop* loop_;
    sockaddr_in addr_;
    int fd_;
    int error_count_;

    ~HttpConnectRequest();

    void setType(IoType type);
    void setFd(int fd);
    void setAddr(const char* ip, int port);
    void setLoop(EventLoop* loop);
    void onComplete(int res_bytes);
    void reset();
};

};

};