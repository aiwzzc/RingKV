#pragma once

#include "src/common.h"
#include "TcpServer.h"
#include "EventLoop.h"
#include "src/jemalloc.h"
#include "PendingWrite.h"

#include <string>
#include <iostream>
#include <memory>
#include <cstring>
#include <functional>
#include <unordered_map>

#define HTTP_RESPONSE_JSON_MAX 4096
#define HTTP_RESPONSE_JSON                                                     \
    "HTTP/1.1 200 OK\r\n"                                                      \
    "Connection:close\r\n"                                                     \
    "Content-Length:%d\r\n"                                                    \
    "Content-Type:application/json;charset=utf-8\r\n\r\n%s"

#define HTTP_RESPONSE_WITH_CODE                                                     \
    "HTTP/1.1 %d %s\r\n"                                                      \
    "Connection:close\r\n"                                                     \
    "Content-Length:%d\r\n"                                                    \
    "Content-Type:application/json;charset=utf-8\r\n\r\n%s"

// 86400单位是秒，86400换算后是24小时
#define HTTP_RESPONSE_WITH_COOKIE                                                    \
    "HTTP/1.1 %d %s\r\n"                                                      \
    "Connection:close\r\n"                                                     \
    "set-cookie: sid=%s; HttpOnly; Max-Age=86400; SameSite=Strict\r\n" \
    "Content-Length:%d\r\n"                                                    \
    "Content-Type:application/json;charset=utf-8\r\n\r\n%s"


#define HTTP_RESPONSE_HTML_MAX 4096
#define HTTP_RESPONSE_HTML                                                    \
    "HTTP/1.1 200 OK\r\n"                                                      \
    "Connection:close\r\n"                                                     \
    "Content-Length:%d\r\n"                                                    \
    "Content-Type:text/html;charset=utf-8\r\n\r\n%s"


#define HTTP_RESPONSE_BAD_REQ                                                     \
    "HTTP/1.1 400 Bad\r\n"                                                      \
    "Connection:close\r\n"                                                     \
    "Content-Length:%d\r\n"                                                    \
    "Content-Type:application/json;charset=utf-8\r\n\r\n%s"

namespace AeroIO {
namespace net {

class TcpConnection;
using TcpConnectionPtt = std::shared_ptr<TcpConnection>;
class UringBuffer;
class EventLoop;
class TcpServer;

};
};

class HttpRequest;
class HttpResponse;

namespace rkv {

class JemallocWrapper;
class Ringengine;
class Config;

};

class HttpServer {

public:
    using HttpCallback = std::function<void(const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest&)>;
    using UpgradeCallback = std::function<void(const AeroIO::net::TcpConnectionPtr&, const HttpRequest&)>;
    using ThreadInitCallback = std::function<void(AeroIO::net::EventLoop*)>;

    HttpServer();
    ~HttpServer();

    void start();
    void setHttpCallback(const HttpCallback& cb);
    void setUpgradeCallback(const UpgradeCallback& cb);
    void setLoopHttpConnectCallback();
    AeroIO::net::TcpConnectionPtr getServerConn() const;

private:
    void onConnection(const AeroIO::net::TcpConnectionPtr& conn);
    void onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::UringBuffer*);
    void onRequest(const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest& request);
    void defaultHttpCallback(const AeroIO::net::TcpConnectionPtr&, const HttpRequest&);

    std::unique_ptr<rkv::JemallocWrapper> mempool_;
    rkv::ServerContext Serverctx_;
    std::unique_ptr<AeroIO::net::TcpServer> server_;
    AeroIO::net::TcpConnectionPtr serverConn_;
    HttpCallback httpCallback_;
    UpgradeCallback upgradeCallback_;
};