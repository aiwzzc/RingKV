#include "http/HttpServer.h"
#include <thread>
#include <vector>

int main() {

    std::vector<std::thread> threads_;

    for(int i = 0; i < 4; ++i) {
        threads_.emplace_back([] () {
            HttpServer server;

            server.start();
        });
    }

    for(auto& t : threads_) {
        if(t.joinable()) t.join();
    }

    return 0;
}