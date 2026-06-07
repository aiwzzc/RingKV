#include "kvserver.h"
#include "config.h"

#include "Log/LoggerManager.h"

using namespace AeroIO::Logger;

#if 0
#include <atomic>    

    std::atomic<uint64_t> g_req_parase{0};
    std::atomic<uint64_t> g_slot_create{0};
    std::atomic<uint64_t> g_slot_send{0};
    std::atomic<uint64_t> g_current_slot{0};
    std::atomic<uint64_t> g_target_slot{0};
    std::atomic<uint64_t> g_target_fill_slot{0};
    std::atomic<uint64_t> g_total_fill_slot{0};
    std::atomic<uint64_t> g_total_fill_slot_all{0};
    std::atomic<uint64_t> g_current_fill_slot{0};

#endif

int main(int argc, char* argv[]) {

    LoggerManager::Instance().start();

    if(argc < 3) return -1;

    rkv::ConfigBuilder builder(argc, argv);
    rkv::Config config = builder.Build();

    rkv::RingKVServer server{config};
    server.start();
    
    return 0;
}