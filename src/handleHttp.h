#pragma once

#include <memory>

class HttpRequest;

namespace AeroIO {
namespace net {

class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

};
};

namespace rkv {

void handleHttp(const AeroIO::net::TcpConnectionPtr& conn, const HttpRequest& req);

};