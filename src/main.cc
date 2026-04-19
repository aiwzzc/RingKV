#include "server.h"

int main() {

    rkv::RingKVServer server{};

    server.start();
    
    return 0;
}