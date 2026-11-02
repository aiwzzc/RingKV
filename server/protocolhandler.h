#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "ringClient.h"

namespace rkv {
class Ringengine;
class RouteBatchTaskPool;
class ShardRouter;
struct CommandContext;
struct CommandDef;

struct delay_write {
    std::vector<std::string_view> tokens_;
    std::vector<uint64_t> slot_indexs_;
    const CommandDef* cmd_def_ = nullptr;
};

class KvsProtocolHandler {
public:
    KvsProtocolHandler(Ringengine& engine, RouteBatchTaskPool& pool, ShardRouter& router);
    int handleProto(const AeroIO::net::TcpConnectionPtr& connection, AeroIO::net::Buffers& buffers);

private:
    int try_parse_resp(const char*, std::size_t, std::size_t&, std::vector<std::string_view>&);
    static void executeCommand(CommandContext&, const CommandDef*);
    void dispatch_command(const ringClientPtr&, std::vector<std::string_view>&,
        const CommandDef*, uint64_t, const Args&,
        std::unordered_map<std::string_view, std::string>&,
        std::unordered_map<std::string_view, delay_write>&);

    Ringengine& engine_;
    RouteBatchTaskPool& task_pool_;
    ShardRouter& router_;
};
}
