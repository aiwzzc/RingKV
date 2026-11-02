#pragma once

#include <vector>
#include <deque>
#include <memory>
#include <variant>

#include "net/TcpConnection.h"

namespace rkv {

struct CommandDef;

struct ResponseSlot {
    uint64_t id_{0};
    bool is_ready_{false};
    std::string data_;
};

using Args = std::variant<AeroIO::net::BlockPtr, std::shared_ptr<const std::string>>;

struct RoutedCommand {
    std::vector<std::string_view> tokens_;
    uint64_t slot_id_;
    Args buffer_;
    const rkv::CommandDef* cmd_def_;
    std::string response_data_;
};

using RouteCommandVec = std::vector<RoutedCommand>;
using RouteCommandPerLoop = std::vector<RouteCommandVec>;

class ringClient {

private:
    std::weak_ptr<AeroIO::net::TcpConnection> conn_;
    RouteCommandPerLoop route_cmds_per_loop_;
    std::deque<ResponseSlot> pending_responses_;
    std::string fragmented_input_;
    uint64_t next_slot_id_{1};

public:
    friend struct RouteBatchTask;

    ringClient(const AeroIO::net::TcpConnectionPtr& conn, std::size_t loops);
    ~ringClient();

    void release();
    std::string& fragmentedInput() { return fragmented_input_; }
    void fillSingleSlot(uint64_t slot_id, std::string&& data, int num);
    uint64_t appendPendRes(ResponseSlot& slot);
    void tryFlushResponses();
    int slot_size();
    bool slot_is_ready(int slot_id);

    RouteCommandPerLoop& route_cmds_per_loop();
};

using ringClientPtr = std::shared_ptr<ringClient>;

};