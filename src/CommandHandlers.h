#pragma once

#include <vector>
#include <functional>
#include <string>
#include <atomic>
#include <memory>
#include <variant>
#include <unordered_map>
#include "dict.h"

namespace AeroIO {

namespace net {

class EventLoop;
class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
struct BufferBlock;
using BlockPtr = std::shared_ptr<BufferBlock>;

};

};

namespace rkv {

class Ringengine;

struct CommandContext {

    std::vector<std::string_view>& tokens_;
    Ringengine* engine_;
    bool is_runInloop_;
    AeroIO::net::EventLoop* curr_loop_;
    AeroIO::net::EventLoop* target_loop_;
    std::string response_;
    bool success_;

};

enum class KEYTYPE {
    SINGLE_KEY, MULTI_KEY, GLOBAL_KEY
};

struct CommandDef {

    std::string name_;
    int arity_;
    bool is_write_;
    bool has_key_;
    int start_key_index_;

    KEYTYPE key_type_; 
    int key_step_;

    std::function<void(CommandContext&)> handle_;

};

using Args = std::variant<AeroIO::net::BlockPtr, std::string>;

struct SubTask {
    std::vector<std::string_view> keys_;
    std::vector<int> original_key_index_;
    Args buffer_;
};

struct GlobalSubTask {
    std::vector<std::string_view> tokens_;
    Args buffer_;
};

struct GlobalKeyCoordinator : public std::enable_shared_from_this<GlobalKeyCoordinator> {
    using GlobalCoordinatorPtr = std::shared_ptr<GlobalKeyCoordinator>;

    AeroIO::net::TcpConnectionPtr conn_;
    int slot_id_;
    std::string cmd_name_;
    std::atomic<int> pending_tasks_{0};
    std::unordered_map<Ringengine*, std::vector<std::string>> keys_results_;

    std::unordered_map<std::string, std::function<void(GlobalSubTask&, Ringengine*, const GlobalCoordinatorPtr&)>> handle_;

    GlobalKeyCoordinator(AeroIO::net::TcpConnectionPtr conn, int slot_id, const std::string& cmd_name);

    void onComplete();

private:
    void finalizeResponse();
};

struct MultiKeyCoordinator : public std::enable_shared_from_this<MultiKeyCoordinator> {

    using MultiCoordinatorPtr = std::shared_ptr<MultiKeyCoordinator>;

    AeroIO::net::TcpConnectionPtr conn_;
    int slot_id_;
    std::string cmd_name_;
    std::atomic<int> pending_tasks_{0};
    std::atomic<int> delete_count_{0};
    std::atomic<int> exists_count_{0};
    std::vector<std::string> mget_results_;

    std::unordered_map<std::string, std::function<void(SubTask&, Ringengine*, const MultiCoordinatorPtr&)>> handle_;

    MultiKeyCoordinator(AeroIO::net::TcpConnectionPtr conn, int slot_id, const std::string& cmd_name, int total_keys);

    void onComplete();

private:
    void finalizeResponse();
};

class CommandRegister {

private:
    rhash_sec<std::string, CommandDef> cmd_table_;

public:
    CommandRegister();

    void registerCmd(CommandDef);
    const CommandDef* lookup(std::string_view name);
    static CommandRegister& instance();

};

class CommandHandlers {

private:
    static bool timeout_delete(std::string_view key, Ringengine* engine);
    static void timeout_delete(std::string_view key);
    static bool timeout_exist(std::string_view key);
    static bool timeout_delete_expire(std::string_view key);
    static void timeout_exist_delete(std::string_view key);

public:
    static void commandCommand(CommandContext& ctx);
    static void pingCommand(CommandContext& ctx);
    static void expireCommand(CommandContext& ctx);
    static void bgsaveCommand(CommandContext& ctx);
    static void infoCommand(CommandContext& ctx);
    static void flushallCommand(CommandContext& ctx);
    static void keysCommand(GlobalSubTask& task, Ringengine* engine, GlobalKeyCoordinator::GlobalCoordinatorPtr coordinator);

    static void existsCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator);
    static void delCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator);
    static void mgetCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator);
    static void setCommand(CommandContext& ctx);
    static void setnxCommand(CommandContext& ctx);
    static void getCommand(CommandContext& ctx);
    static void incrCommand(CommandContext& ctx);
    static void incrbyCommand(CommandContext& ctx);
    static void decrCommand(CommandContext& ctx);
    static void decrbyCommand(CommandContext& ctx);
    
    static void lpushCommand(CommandContext& ctx);
    static void rpushCommand(CommandContext& ctx);
    static void lpopCommand(CommandContext& ctx);
    static void rpopCommand(CommandContext& ctx);
    static void lsetCommand(CommandContext& ctx);
    static void lindexCommand(CommandContext& ctx);
    static void llenCommand(CommandContext& ctx);
    static void lrangeCommand(CommandContext& ctx);

    static void zaddCommand(CommandContext& ctx);
    static void zrevrangeCommand(CommandContext& ctx);
    static void zrangeCommand(CommandContext& ctx);
    static void zscoreCommand(CommandContext& ctx);
    static void zcardCommand(CommandContext& ctx);
    static void zrankCommand(CommandContext& ctx);
    static void zrevrankCommand(CommandContext& ctx);

};

};