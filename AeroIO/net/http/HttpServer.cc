#include "HttpServer.h"
#include "HttpRequest.h"
#include "HttpContext.h"
#include "HttpResponse.h"
#include "EventLoop.h"
#include "TcpConnection.h"
#include "src/config.h"
#include "src/server.h"
#include "PendingWrite.h"

#include <fstream>
#include <sstream>

#if 0

HttpServer::HttpServer() : mempool_(std::make_unique<rkv::JemallocWrapper>()),
    Serverctx_(this->mempool_.get(), nullptr, nullptr),
    server_(std::make_unique<AeroIO::net::TcpServer>(&this->Serverctx_, rkv::Config::getInstance().httpServer_port)) {
    this->server_->setMessageCallback([this] (const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::UringBuffer* buf) {
        onMessage(conn, buf);
    });

    this->server_->setConnectionCallback([this] (const AeroIO::net::TcpConnectionPtr& conn) { onConnection(conn); });
    setHttpCallback([this] (const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest& req) { defaultHttpCallback(conn, req); });

}

HttpServer::~HttpServer() = default;

void HttpServer::start() {
    this->server_->getLoop()->startConnectHttp();
    setLoopHttpConnectCallback();
    this->server_->start();
}

void HttpServer::setHttpCallback(const HttpCallback& cb)
{ this->httpCallback_ = std::move(cb); }

void HttpServer::setUpgradeCallback(const UpgradeCallback& cb)
{ this->upgradeCallback_ = std::move(cb); }

void HttpServer::setLoopHttpConnectCallback() {
    this->server_->getLoop()->setHttpConnectCallback([this] (const AeroIO::net::TcpConnectionPtr& conn) {
        this->serverConn_ = conn;
    });
}

AeroIO::net::TcpConnectionPtr HttpServer::getServerConn() const
{ return this->serverConn_; }

void HttpServer::onConnection(const AeroIO::net::TcpConnectionPtr& conn) {
    conn->setTcpNoDelay(true);
    conn->setContext(HttpContext{});

}

void HttpServer::onMessage(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::UringBuffer* buf) {
    if(conn->disconnected()) return;
std::cout << std::string_view(buf->peek(), buf->readableBytes()) << std::endl;
    HttpContext* context = std::any_cast<HttpContext>(conn->getMutableContext());
    if(context == nullptr) return;

    if(!context->parseRequest(buf)) {
        std::string_view badStr("HTTP/1.1 400 Bad Request\r\n\r\n");
        std::cout << badStr << std::endl;
        conn->send(badStr.data(), badStr.size());
        conn->shutdown();
        return;
    }

    if(context->gotAll()) {
        auto connection_opt = context->request().getHeader("Connection");

        if(connection_opt.has_value() && (*connection_opt.value()).find("Upgrade") != std::string::npos) {
            if(this->upgradeCallback_) {
                this->upgradeCallback_(conn, context->request());

            } else {
                // 返回 501 Not Implemented 拒绝
            }

        } else {
            onRequest(conn, context->request());
            if(connection_opt.has_value() && *connection_opt.value() == "keep-alive") context->reset();
        }
    }
}

void HttpServer::onRequest(const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest& request) {
    if(conn->disconnected()) return;

    this->httpCallback_(conn, request);
}

void HttpServer::defaultHttpCallback(const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest& req) {
    if(conn->disconnected()) return;

    HttpResponse res{true};
    res.setStatusCode(HttpResponse::HttpStatusCode::k404NotFound);
    res.setStatusMessage("Not Found");
    res.setCloseConnection(true);
    
    std::string res_json{};
    res.appendToBuffer(res_json);
    conn->send(res_json.data(), res_json.size());
}

#endif