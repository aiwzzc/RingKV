#pragma once

#include "jemalloc.h"

#include <string>
#include <unordered_map>

#include "Buffer.h"
#include "EventLoop.h"
#include "Callbacks.h"
#include "Acceptor.h"
#include "PendingWrite.h"


namespace AeroIO {
namespace net {

enum class AcceptorOption { kNoReusePort, kReusePort };

class TcpServer {

public:
    TcpServer(base::JemallocWrapper*, int, AcceptorOption option = AcceptorOption::kReusePort);

    ~TcpServer();
    void prepare(const std::string& ip);
    void start();
    void quiesce() noexcept;
    EventLoop* getLoop();

    void setMessageCallback(const MessageCallback&);
    void setConnectionCallback(const ConnectionCallback&);
    void setWriteCompleteCallback(const WriteCompleteCallback&);

    TcpConnectionPtr addNewConnection(int sockfd);
    void removeConnection(int sockfd);
    void addConnections(int sockfd, TcpConnectionPtr conn);

private:

    base::JemallocWrapper* mempool_;
    EventLoop loop_{};
    Acceptor acceptor_;
    ReplyBufferPool replyBufferPool_;
    BlockPool blockPool_;

    std::string name_;

    MessageCallback messageCallback_;
    ConnectionCallback connectionCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    std::unordered_map<int, TcpConnectionPtr> connections_;
};

};
};