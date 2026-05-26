#pragma once

#include <memory>

#include "net/PendingWrite.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/TcpConnection.h"

namespace rkv {

// ============ RouteBatchTask =============

using Args = std::variant<AeroIO::net::BlockPtr, std::string>;

struct RoutedCommand {
    std::vector<std::string_view> tokens_;
    uint64_t slot_id_;
    Args buffer_;
    const rkv::CommandDef* cmd_def_;
    std::string response_data_;
};

struct CommandContext;
struct CommandDef;

using execute_cmd = void(*)(rkv::CommandContext&, const rkv::CommandDef*);

struct RouteBatchTask {

    struct AeroIO::net::list_head node_;

    RoutedCommand cmds[64];
    int cmd_count_{0};

    AeroIO::net::TcpConnectionPtr conn_;
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
    struct AeroIO::net::list_head head_;
    std::size_t pool_size_;
    rkv::JemallocWrapper* mempool_;

public:
    RouteBatchTaskPool();
    ~RouteBatchTaskPool();

    static RouteBatchTaskPool& getInstance();
    RouteBatchTaskPtr get(AeroIO::net::EventLoop* current_loop);
    void release(RouteBatchTask* task);
    void initPool();
    void setMempool(rkv::JemallocWrapper* mempool);

};

};