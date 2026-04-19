#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "EventLoop.h"
#include "Callbacks.h"
#include "Acceptor.h"

namespace rkv {

class JemallocWrapper;
class Ringengine;
class Config;
class TcpConnection;
struct ServerContext;

};

namespace AeroIO {

namespace net {

class ReplyBufferPool;

enum class Option { kNoReusePort, kReusePort };

class TcpServer {

public:
    friend class TcpConnection;

    using LoopsEngines = std::vector<std::pair<EventLoop*, rkv::Ringengine*>>*;

    TcpServer(rkv::ServerContext*, int, Option option = Option::kReusePort);

    void start();
    EventLoop* getLoop() const;
    void setLoopsEngines(LoopsEngines LoopsEngines);
    LoopsEngines getLoopsEngines();
    rkv::ServerContext* getServerContext();

    void setMessageCallback(const MessageCallback&);
    void setConnectionCallback(const ConnectionCallback&);
    void setWriteCompleteCallback(const WriteCompleteCallback&);

    TcpConnectionPtr addNewConnection(int sockfd);
    void removeConnection(int sockfd);
    void addConnections(int sockfd, TcpConnectionPtr conn);

private:

    rkv::ServerContext* Serverctx_;
    std::unique_ptr<EventLoop> loop_;
    std::unique_ptr<Acceptor> acceptor_;
    std::string name_;
    LoopsEngines LoopsEngines_;

    MessageCallback messageCallback_;
    ConnectionCallback connectionCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    std::unordered_map<int, TcpConnectionPtr> connections_;
};

};

};