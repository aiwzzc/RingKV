#pragma once

#include <memory>

#include "ringClient.h"

#include "net/PendingWrite.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/TcpConnection.h"

namespace rkv {

// ============ RouteBatchTask =============

struct CommandContext;
struct CommandDef;

using execute_cmd = void(*)(rkv::CommandContext&, const rkv::CommandDef*);

struct RouteBatchTask : std::enable_shared_from_this<RouteBatchTask> {



    RoutedCommand cmds[64];
    int cmd_count_{0};

    ringClientPtr clinet_;
    rkv::Ringengine* target_engine_;
    AeroIO::net::EventLoop* target_loop_;
    AeroIO::net::EventLoop* current_loop_;
    execute_cmd execute_;

    void reset();
    void operator()();
    
};

using RouteBatchTaskPtr = std::shared_ptr<RouteBatchTask>;

constexpr std::size_t TASKPOOLSIZE = 512;

class RouteBatchTaskPool {

private:
    std::vector<RouteBatchTask*> free_;
    rkv::JemallocWrapper& mempool_;

public:
    explicit RouteBatchTaskPool(rkv::JemallocWrapper& mempool);
    RouteBatchTaskPool(const RouteBatchTaskPool&) = delete;
    RouteBatchTaskPool& operator=(const RouteBatchTaskPool&) = delete;
    ~RouteBatchTaskPool();

    RouteBatchTaskPtr get(AeroIO::net::EventLoop* current_loop);
    void release(RouteBatchTask* task);

};

};