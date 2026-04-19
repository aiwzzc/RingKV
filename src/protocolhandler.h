#pragma once

#include <vector>
#include <string>
#include <memory>
#include <deque>
#include <variant>
#include <unordered_map>

namespace AeroIO {
namespace net {

class TcpConnection;
class UringBuffer;
struct ReplyBuffer;
class EventLoop;
struct ResponseSlot;
struct BufferBlock;
using BlockPtr = std::shared_ptr<BufferBlock>;
using Buffers = std::deque<BlockPtr>;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

} // namespace net
} // namespace AeroIO

namespace rkv {

class Ringengine;
struct RedisObject;
struct CommandContext;
struct CommandDef;
struct SubTask;
struct MultiKeyCoordinator;

using Args = std::variant<AeroIO::net::BlockPtr, std::string>;
using LoopsEngines = std::vector<std::pair<AeroIO::net::EventLoop*, Ringengine*>>*;

struct delay_write {
    std::vector<std::string_view> tokens_;
    std::vector<int> slot_indexs_;
    CommandDef* cmd_def_;
};

class KvsProtocolHandler {
private:

    Ringengine* engine_;
    int dispatch(const AeroIO::net::TcpConnectionPtr&, std::vector<std::string_view>&, AeroIO::net::ReplyBuffer*);
    int try_parse_resp(const char*, std::size_t, std::size_t&, std::vector<std::string_view>&);
    static void executeCommandAndPersist(CommandContext& ctx, const CommandDef* def);
    void dispatch_command(const AeroIO::net::TcpConnectionPtr& conn, std::vector<std::string_view>& tokens, 
        const CommandDef* cmd_def, int current_slot_id, const Args& args, bool need_reply, void* ptr, 
        std::unordered_map<std::string_view, std::string>&, std::unordered_map<std::string_view, delay_write>&);

public:
    explicit KvsProtocolHandler(Ringengine* engine);

    int handleProto(const AeroIO::net::TcpConnectionPtr&, AeroIO::net::Buffers&);
    static std::string dumpObjectToResp(const std::string_view& key, RedisObject* obj);
    static void dumpObjToBinaryRdb(AeroIO::net::UringBuffer* buffer, const std::string_view& key, RedisObject* obj);
};

}