#include "kvserver.h"
#include "Log/LoggerManager.h"

using namespace AeroIO::Logger;

int main() {

    LoggerManager::Instance().start();

    rkv::RingKVServer server{};
    server.start();
    
    return 0;
}