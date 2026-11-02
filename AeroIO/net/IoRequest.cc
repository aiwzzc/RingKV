#include "IoRequest.h"
#include "TcpConnection.h"

#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace AeroIO {

namespace net {

IoRequest::IoRequest(TcpConnectionPtr conn) : conn_(conn) {};

IoRequest::~IoRequest() = default;

void IoRequest::setType(IoType type)
{ this->head_.type_ = type; }

void IoRequest::setConn(TcpConnectionPtr conn)
{ this->conn_ = conn; }

void IoRequest::onComplete(int res_bytes) {

    auto conn = this->conn_.lock();
    if(conn && this->head_.type_ == IoType::RECV) {
        conn->handleRead(res_bytes);
    }
}

TimerRequest::TimerRequest() : head_{IoType::TIMER}, ts_{} {}

void TimerRequest::setTimer(uint64_t ms, std::function<void()> cb) {
    this->callback_ = cb;
    ts_.tv_sec = ms / 1000;
    ts_.tv_nsec = (ms % 1000) * 1000000;
}

void TimerRequest::setType(IoType type)
{ this->head_.type_ = type; }

void TimerRequest::onComplete(int res_bytes) {

    if(this->head_.type_ == IoType::TIMER) {
        if(res_bytes == 0 || res_bytes == -ETIME) {
            auto callback = callback_;
            if (callback) callback();
        }
    }
}

}
}
