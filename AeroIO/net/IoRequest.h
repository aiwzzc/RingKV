#pragma once

#include <memory>
#include <sys/uio.h>
#include <functional>
#include <linux/time_types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace AeroIO {

namespace net {

enum class IoType { ACCEPT, RECV, SEND, TIMER };

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

struct TimerRequest {

    GeneralHead head_;
    struct __kernel_timespec ts_;
    std::function<void()> callback_;

    TimerRequest();
    void setType(IoType type);
    void setTimer(uint64_t ms, std::function<void()> cb);
    void onComplete(int res_bytes);
};

}
}
