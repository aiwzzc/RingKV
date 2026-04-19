#include "TcpServer.h"
#include "TcpConnection.h"
#include "Buffer.h"
#include <iostream>
#include <string>
#include <liburing.h>
#include <csignal>
#include <thread>
#include <vector>
#include <cstring>

using AeroIO::net::TcpServer;
using AeroIO::net::TcpConnection;
using AeroIO::net::TcpConnectionPtr;
using AeroIO::net::UringBuffer;

AeroIO::net::TcpServer* g_server = nullptr;

// 信号处理回调函数
void signal_handler(int sig) {
    std::cout << "\n[AeroIO] Received signal " << sig << ", shutting down gracefully...\n";
    if (g_server) {
        // 通知底层 EventLoop 退出循环
        g_server->getLoop()->quit(); 
    }
}

int main() {

    TcpServer server;
    const char* response =  "HTTP/1.1 200 OK\r\n"                                                      
                            "Connection:close\r\n"                                                     
                            "Content-Length:0\r\n"                                                    
                            "Content-Type:application/json;charset=utf-8\r\n\r\n";
    // g_server = &server;

    // std::signal(SIGINT, signal_handler);
    // std::signal(SIGTERM, signal_handler);

    server.setMessageCallback([&response] (const TcpConnectionPtr& conn, UringBuffer* buffer) {
        // std::cout << std::string(buffer->peek(), buffer->readableBytes()) << std::endl;

        conn->send(response);
        buffer->retrieveAll();
    });

    server.start();

    // std::vector<std::thread> threads_;

    // for(int i = 0; i < 2; ++i) {
    //     threads_.emplace_back([] () {
    //         TcpServer server;

    //         server.setMessageCallback([] (const TcpConnectionPtr& conn, UringBuffer* buffer) {
    //             // std::cout << std::string(buffer->peek(), buffer->readableBytes()) << std::endl;

    //             const char* response =  "HTTP/1.1 200 OK\r\n"                                                      
    //                                     "Connection:close\r\n"                                                     
    //                                     "Content-Length:0\r\n"                                                    
    //                                     "Content-Type:application/json;charset=utf-8\r\n\r\n";

    //             conn->send(response, strlen(response));
    //             buffer->retrieveAll();
    //         });

    //         server.start();
    //     });
    // }

    // for(auto& t : threads_) {
    //     if(t.joinable()) t.join();
    // }

    return 0;
}