#pragma once

#include <string>
#include <unordered_map>

#include "Buffer.h"
#include "EventLoop.h"
#include "Callbacks.h"
#include "Acceptor.h"
#include "PendingWrite.h"

namespace rkv {

class JemallocWrapper;
class Ringengine;
class TcpConnection;

};

namespace AeroIO {
namespace net {

enum class Option { kNoReusePort, kReusePort };

class TcpServer {

public:
    friend class TcpConnection;

    using LoopsEngines = std::vector<std::pair<EventLoop*, rkv::Ringengine*>>*;

    TcpServer(rkv::JemallocWrapper*, int, Option option = Option::kReusePort);

    void start();
    EventLoop* getLoop();
    void setLoopsEngines(LoopsEngines LoopsEngines);
    LoopsEngines getLoopsEngines();

    void setMessageCallback(const MessageCallback&);
    void setConnectionCallback(const ConnectionCallback&);
    void setWriteCompleteCallback(const WriteCompleteCallback&);

    TcpConnectionPtr addNewConnection(int sockfd);
    void removeConnection(int sockfd);
    void addConnections(int sockfd, TcpConnectionPtr conn);

private:

    rkv::JemallocWrapper* mempool_;
    EventLoop loop_{};
    Acceptor acceptor_;
    ReplyBufferPool replyBufferPool_;
    BlockPool blockPool_;

    std::string name_;
    LoopsEngines LoopsEngines_;

    MessageCallback messageCallback_;
    ConnectionCallback connectionCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    std::unordered_map<int, TcpConnectionPtr> connections_;
};

};
};