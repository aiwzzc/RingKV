#include "protocolhandler.h"
#include "RouteBatchTask.h"
#include "ShardRouter.h"
#include "CommandHandlers.h"
#include "core/engine.h"
#include "respresstr.h"

#include <algorithm>
#include <any>
#include <charconv>
#include <cctype>

namespace rkv {
KvsProtocolHandler::KvsProtocolHandler(Ringengine& engine, RouteBatchTaskPool& pool, ShardRouter& router) :
    engine_(engine), task_pool_(pool), router_(router) {}

int KvsProtocolHandler::try_parse_resp(const char* buffer, std::size_t len, size_t& pos, std::vector<std::string_view>& tokens) {
    if(pos >= len) return 0;

    std::string_view buf(buffer, len);

    if(buf[pos] != '*') return -1;
    pos++;

    size_t line_end = buf.find("\r\n", pos);
    if(line_end == std::string::npos) return 0;

    int total_len = 0;

    auto [p, ec] = std::from_chars(buf.data() + pos, buf.data() + line_end, total_len);
    if(ec != std::errc() || total_len < 0) return -1;

    if(total_len < 0) return -1;

    pos = line_end + 2;

    for(int i = 0; i < total_len; ++i) {
        if(pos >= buf.size()) return 0;
        if(buf[pos] != '$') return -1;

        pos++;

        line_end = buf.find("\r\n", pos);
        if(line_end == std::string::npos) return 0;

        int cmd_len = 0;
        auto [p2, ec2] = std::from_chars(buf.data() + pos, buf.data() + line_end, cmd_len);
        if(ec2 != std::errc()) return -1;

        if(cmd_len == -1) {
            tokens.push_back("");
            pos = line_end + 2;
            continue;
        }

        if(cmd_len < 0) return -1;

        pos = line_end + 2;

        if(buf.size() < pos + cmd_len + 2) return 0;

        tokens.emplace_back(buf.data() + pos, cmd_len);
        pos += cmd_len + 2;
    }

    return 1;
}

void KvsProtocolHandler::executeCommand(CommandContext& ctx, const CommandDef* command) {
    command->handle_(ctx);
}

void KvsProtocolHandler::dispatch_command(
    const ringClientPtr& client, std::vector<std::string_view>& tokens,
    const CommandDef* command, uint64_t slot, const Args& input,
    std::unordered_map<std::string_view, std::string>& reads,
    std::unordered_map<std::string_view, delay_write>& writes) {
    if (!command->has_key_) {
        CommandContext ctx{tokens, &engine_, router_, "", false};
        executeCommand(ctx, command);
        client->fillSingleSlot(slot, std::move(ctx.response_), 1);
        return;
    }
    if (command->key_type_ != KEYTYPE::SINGLE_KEY) {
        client->fillSingleSlot(slot, "-ERR command is not supported in this build\r\n", 1);
        return;
    }
    const auto shard = router_.shardFor(tokens[command->start_key_index_]);
    if (!router_.isLocal(shard)) {
        client->route_cmds_per_loop()[shard].emplace_back(std::move(tokens), slot, input, command);
        return;
    }
    if (!command->is_write_ && reads.contains(tokens[1])) {
        client->fillSingleSlot(slot, std::string(reads.at(tokens[1])), 2);
    } else if (command->is_write_) {
        auto& write = writes[tokens[1]];
        write.slot_indexs_.push_back(slot);
        write.tokens_ = std::move(tokens);
        write.cmd_def_ = command;
    } else {
        CommandContext ctx{tokens, &engine_, router_, "", false};
        executeCommand(ctx, command);
        reads[tokens[1]] = ctx.response_;
        client->fillSingleSlot(slot, std::move(ctx.response_), 3);
    }
}

int KvsProtocolHandler::handleProto(const AeroIO::net::TcpConnectionPtr& conn,
                                  AeroIO::net::Buffers& blocks) {
    // Protocol state belongs to a connection's application session, not TCP.
    auto* stored = std::any_cast<ringClientPtr>(conn->getMutableContext());
    if (!stored) {
        conn->setContext(std::make_shared<ringClient>(conn, router_.size()));
        stored = std::any_cast<ringClientPtr>(conn->getMutableContext());
    }
    const auto client = *stored;
    auto& fragmented = client->fragmentedInput();
    auto& registry = CommandRegister::instance();
    auto* owner_loop = conn->getLoop();

    // Local delayed commands and cache keys borrow these inputs until this batch ends.
    std::vector<Args> retained_inputs;
    std::unordered_map<std::string_view, std::string> reads;
    std::unordered_map<std::string_view, delay_write> writes;
    bool malformed = false;

    auto parse = [&](std::string_view input, const Args& owner) {
        retained_inputs.push_back(owner);
        std::size_t consumed = 0;
        while (consumed < input.size()) {
            const auto command_start = consumed;
            std::vector<std::string_view> tokens;
            const int status = try_parse_resp(input.data(), input.size(), consumed, tokens);
            if (status == 0) {
                fragmented.assign(input.substr(command_start));
                break;
            }
            if (status < 0) {
                ResponseSlot response;
                const auto slot = client->appendPendRes(response);
                client->fillSingleSlot(slot, PROTOCOLERRORSTR, 0);
                malformed = true;
                break;
            }
            if (tokens.empty()) continue;
            std::string name(tokens[0]);
            for (char& byte : name) byte = static_cast<char>(std::tolower(static_cast<unsigned char>(byte)));
            const auto* command = registry.lookup(name);
            ResponseSlot response;
            const auto slot = client->appendPendRes(response);
            if (!command) {
                client->fillSingleSlot(slot, UNKNOWNCOMMANDSTR, 0);
            } else if ((command->arity_ > 0 && tokens.size() != command->arity_) ||
                       (command->arity_ < 0 && tokens.size() < -command->arity_)) {
                client->fillSingleSlot(slot, WRONGNUMBERSTR, 0);
            } else {
                dispatch_command(client, tokens, command, slot, owner, reads, writes);
            }
        }
    };

    for (auto& block : blocks) {
        if (block->readableBytes() == 0) continue;
        if (!fragmented.empty()) {
            fragmented.append(block->peek(), block->readableBytes());
            // A routed string_view must keep the exact immutable string it refers to.
            auto input = std::make_shared<const std::string>(std::move(fragmented));
            fragmented.clear();
            parse(*input, Args{input});
        } else {
            parse(std::string_view(block->peek(), block->readableBytes()), Args{block});
        }
        block->retrieveAll();
        if (malformed) break;
    }

    for (auto& [key, write] : writes) {
        CommandContext ctx{write.tokens_, &engine_, router_, "", false};
        executeCommand(ctx, write.cmd_def_);
        for (auto slot : write.slot_indexs_)
            client->fillSingleSlot(slot, std::string(ctx.response_), 4);
    }

    for (std::size_t shard = 0; shard < router_.size(); ++shard) {
        auto& commands = client->route_cmds_per_loop()[shard];
        for (std::size_t offset = 0; offset < commands.size();) {
            auto task = task_pool_.get(owner_loop);
            task->clinet_ = client;
            task->current_loop_ = owner_loop;
            task->router_ = &router_;
            task->execute_ = executeCommand;
            task->cmd_count_ = static_cast<int>(std::min(std::size(task->cmds), commands.size() - offset));
            for (int i = 0; i < task->cmd_count_; ++i) task->cmds[i] = std::move(commands[offset++]);
            router_.dispatch(shard, [task](Ringengine& engine) {
                task->target_engine_ = &engine;
                (*task)();
            });
        }
        commands.clear();
    }
    client->tryFlushResponses();
    return malformed ? -1 : 0;
}
}
