#pragma once

#include <functional>
#include <memory>
#include <sys/socket.h>
#include <deque>
#include <netinet/in.h>

namespace AeroIO {

namespace net {

class UringBuffer;
class TcpConnection;
struct BufferBlock;
using BlockPtr = std::shared_ptr<BufferBlock>;
using Buffers = std::deque<BlockPtr>;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffers&)>;
using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
using NewConnectionCallback = std::function<void(int)>;
using TimerCallback = std::function<void()>;

};

};