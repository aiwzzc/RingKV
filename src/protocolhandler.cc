#include "protocolhandler.h"
#include "AeroIO/net/TcpConnection.h"
#include "AeroIO/net/Buffer.h"
#include "AeroIO/net/PendingWrite.h"
#include "AeroIO/net/EventLoop.h"
#include "engine.h"
#include "kvstr.h"
#include "respresstr.h"
#include "config.h"
#include "CommandHandlers.h"

#include <charconv>
#include <string_view>
#include <cstring>
#include <iostream>
#include <hiredis/hiredis.h>

using AeroIO::net::RouteBatchTask;
using AeroIO::net::RouteBatchTaskPool;

namespace {

    bool is_integer(std::string_view s) {
        if(s.empty()) return false;

        size_t i = 0;
        if(s[0] == '-' || s[0] == '+') {
            if(s.size() == 1) return false;

            ++i;
        }

        for(; i < s.size(); ++i) {
            if(std::isdigit(static_cast<unsigned char>(s[i])) == false) return false;
        }

        return true;
    }

    std::string redisobject_to_resp_string(const std::vector<rkv::RedisObject*>& tokens) {
        std::string res = "*" + std::to_string(tokens.size()) + "\r\n";

        for(const auto& s : tokens) {
            if(s->type == rkv::OBJ_STRING) {
                rkv::kvstr* dataptr = (rkv::kvstr*)s->ptr;
                res += "$" + std::to_string(dataptr->len_) + "\r\n" + std::string(dataptr->buf_, dataptr->len_) + "\r\n";
            }
        }

        return res;
    }

    std::string to_resp_string(const std::vector<std::string_view>& tokens) {
        std::string res = "*" + std::to_string(tokens.size()) + "\r\n";

        for(const auto& s : tokens) {
            res += "$" + std::to_string(s.length()) + "\r\n";
            res += s;
            res += "\r\n";
        }

        return res;
    }

    std::string to_resp_string(const std::vector<std::string>& tokens) {
        std::string res = "*" + std::to_string(tokens.size()) + "\r\n";

        for(const auto& s : tokens) {
            res += "$" + std::to_string(s.length()) + "\r\n";
            res += s + "\r\n";
        }

        return res;
    }

    inline uint64_t stable_hash(std::string_view key) {
        uint64_t hash = 0xcbf29ce484222325;
        for (char c : key) {
            hash ^= c;
            hash *= 0x100000001b3;
        }
        return hash;
    }

    std::string buildSetResp(const std::string_view key, const std::string_view value) {
        char* cmd;
        int len = redisFormatCommand(&cmd, "SET %b %b",
                                    key.data(), (size_t)key.size(),
                                    value.data(), (size_t)value.size());
        if (len < 0) throw std::runtime_error("redisFormatCommand failed");

        std::string resp(cmd, len);
        free(cmd); // hiredis 需要手动释放
        return resp;
    }

    // 辅助函数：将一个 32 位整数以二进制形式写入 string buffer
    void writeInt32(AeroIO::net::UringBuffer* buffer, uint32_t value) {
        buffer->append(reinterpret_cast<char*>(&value), sizeof(value));
    }

    // 辅助函数：带长度前缀的字符串写入
    void writeString(AeroIO::net::UringBuffer* buffer, const std::string_view& str) {
        uint32_t len = str.size();
        writeInt32(buffer, len);
        buffer->append((char*)str.data(), len);
    }

};

using AeroIO::net::ReplyBuffer;
using AeroIO::net::EventLoop;
using AeroIO::net::ResponseSlot;

namespace rkv {

KvsProtocolHandler::KvsProtocolHandler(Ringengine* engine) : engine_(engine) {}

std::string KvsProtocolHandler::dumpObjectToResp(const std::string_view& key, RedisObject* obj) {
    switch(obj->type) {
        case OBJ_STRING: {
            return buildSetResp(key, (char*)obj->ptr);
        }

        default:
            return "";
    }
}

void KvsProtocolHandler::dumpObjToBinaryRdb(AeroIO::net::UringBuffer* buffer, 
    const std::string_view& key, RedisObject* obj) {

    uint8_t type_flag = obj->type;
    buffer->append(reinterpret_cast<char*>(&type_flag), 1);

    writeString(buffer, key);

    if(obj->type == OBJ_STRING) {
        kvstr* s = (kvstr*)obj->ptr;
        writeString(buffer, std::string_view(s->buf_, s->len_));
    }
}

// return: -1 -> error; 0 -> 半包; 1 -> success
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

void KvsProtocolHandler::executeCommandAndPersist(CommandContext& ctx, const CommandDef* def) {
    if(def == nullptr) return;
    
#if 0
    if(!ctx.is_runInloop_ && def->is_write_ && ctx.curr_loop_->rdbIsSaving()) {
        auto& map = ctx.engine_->getUnderlyingMap();
        std::string_view key = ctx.tokens_[def->start_key_index_];
        std::size_t bucket_idx = map.bucket(std::string(key));

        if(ctx.curr_loop_->rdbCowBuchetIndex() < bucket_idx) {
            if(ctx.curr_loop_->rdbCowSavedKeys().find(std::string(key)) == ctx.curr_loop_->rdbCowSavedKeys().end()) {
                RedisObject* obj = ctx.engine_->get(key);

                if(obj) {
                    KvsProtocolHandler::dumpObjToBinaryRdb(ctx.curr_loop_->rdbWriteBuffer(), key, obj);
                    ctx.curr_loop_->rdbCowSavedKeys().insert(std::string(key));
                }
            }
        }
    }
#endif
    def->handle_(ctx);
    
    if(def->is_write_ && ctx.success_) {
        std::string intact_cmd = to_resp_string(ctx.tokens_);

        if(ctx.is_runInloop_) {
            Config& config = Config::getInstance();

            if(config.cluster_enabled_ && config.is_master_) {
                ctx.curr_loop_->appendReplicationBacklog(intact_cmd);
            }

            if(config.aof_enabled_) {
                ctx.target_loop_->appendAof(intact_cmd.data(), intact_cmd.size());
            }


        } else {
            Config& config = Config::getInstance();

            if(config.cluster_enabled_ && config.is_master_) {
                ctx.curr_loop_->appendReplicationBacklog(intact_cmd);
            }

            if(config.aof_enabled_) {
                ctx.curr_loop_->appendAof(intact_cmd.data(), intact_cmd.size());
            }
        }
    }
}

void KvsProtocolHandler::dispatch_command(const AeroIO::net::TcpConnectionPtr& conn, 
    std::vector<std::string_view>& tokens_view, const CommandDef* cmd_def, int current_slot_id, 
    const Args& args, bool need_reply, void* ptr, std::unordered_map<std::string_view, std::string>& local_notwrite_cache, 
    std::unordered_map<std::string_view, delay_write>& local_last_write_slot) {

    std::vector<std::pair<AeroIO::net::EventLoop*, Ringengine*>>* LoopsEngines = 
    (std::vector<std::pair<AeroIO::net::EventLoop*, Ringengine*>>*)ptr;
    EventLoop* curr_loop = EventLoop::getEventLoopOfCurrentThread();

    if(!cmd_def->has_key_) {
        CommandContext ctx{tokens_view, this->engine_, false, curr_loop, curr_loop, "", false};
        this->executeCommandAndPersist(ctx, cmd_def);

        if(need_reply) conn->fillSingleSlot(current_slot_id, std::move(ctx.response_));

    } else {

        if(cmd_def->key_type_ == KEYTYPE::SINGLE_KEY) {
            std::size_t target_index = stable_hash(tokens_view[cmd_def->start_key_index_]) % LoopsEngines->size();
            EventLoop* target_loop = (*LoopsEngines)[target_index].first;

            if(curr_loop == target_loop) {
                if(local_notwrite_cache.contains(tokens_view[1]) && !cmd_def->is_write_) {
                    std::string hot_data = local_notwrite_cache[tokens_view[1]];
                    if(need_reply) conn->fillSingleSlot(current_slot_id, std::move(hot_data));
                    return;

                } else if(cmd_def->is_write_) {
                    auto& delay_write = local_last_write_slot[tokens_view[1]];
                    delay_write.slot_indexs_.push_back(current_slot_id);
                    delay_write.tokens_ = std::move(tokens_view);
                    delay_write.cmd_def_ = (CommandDef*)cmd_def;
                    return;
                }

                CommandContext ctx{tokens_view, this->engine_, false, curr_loop, curr_loop, "", false};
                this->executeCommandAndPersist(ctx, cmd_def);
                if(!cmd_def->is_write_) local_notwrite_cache[tokens_view[1]] = ctx.response_;

                if(need_reply) conn->fillSingleSlot(current_slot_id, std::move(ctx.response_));

            } else {
                conn->route_batches()[target_index].emplace_back(std::move(tokens_view), current_slot_id, args, cmd_def);
            }

        } else if(cmd_def->key_type_ == KEYTYPE::MULTI_KEY) {
            int keys_num = (tokens_view.size() - cmd_def->start_key_index_) / cmd_def->key_step_;

            auto coordinator = std::make_shared<MultiKeyCoordinator>(conn, current_slot_id, cmd_def->name_, keys_num);

            std::unordered_map<EventLoop*, SubTask> batch_tasks_;

            int key_index{0};
            for(int i = cmd_def->start_key_index_; i < tokens_view.size(); i += cmd_def->key_step_) {
                std::string_view key = tokens_view[i];

                std::size_t target_index = stable_hash(key) % LoopsEngines->size();
                EventLoop* target_loop = (*LoopsEngines)[target_index].first;

                batch_tasks_[target_loop].keys_.push_back(key);
                batch_tasks_[target_loop].original_key_index_.push_back(key_index);
                ++key_index;

                batch_tasks_[target_loop].buffer_ = args;
            }

            coordinator->pending_tasks_.store(batch_tasks_.size(), std::memory_order_relaxed);

            for(auto& [target_loop, task] : batch_tasks_) {
                if(target_loop == curr_loop) {
                    coordinator->handle_[coordinator->cmd_name_](task, curr_loop->getEngine(), coordinator);

                } else {
                    target_loop->runInLoop([target_loop, coordinator, task = std::move(task)] () mutable {
                        coordinator->handle_[coordinator->cmd_name_](task, target_loop->getEngine(), coordinator);
                    });
                }
            }

        } else if(cmd_def->key_type_ == KEYTYPE::GLOBAL_KEY) {
            auto coordinator = std::make_shared<GlobalKeyCoordinator>(conn, current_slot_id, cmd_def->name_);
            coordinator->pending_tasks_.store(LoopsEngines->size(), std::memory_order_relaxed);

            std::unordered_map<EventLoop*, GlobalSubTask> batch_tasks_;

            for(auto& [loop, engine] : *LoopsEngines) {
                batch_tasks_[loop].tokens_ = tokens_view;
                batch_tasks_[loop].buffer_ = args;
            }

            for(auto& [target_loop, task] : batch_tasks_) {
                if(target_loop == curr_loop) {
                    coordinator->handle_[coordinator->cmd_name_](task, curr_loop->getEngine(), coordinator);

                } else {
                    target_loop->runInLoop([target_loop, coordinator, task = std::move(task)] () mutable {
                        coordinator->handle_[coordinator->cmd_name_](task, target_loop->getEngine(), coordinator);
                    });
                }
            }
        }
    }
}

int KvsProtocolHandler::handleProto(const AeroIO::net::TcpConnectionPtr& conn, AeroIO::net::Buffers& blocks) {

    auto* LoopsEngines = conn->getLoop()->getLoopsEngines();
    EventLoop* curr_loop = EventLoop::getEventLoopOfCurrentThread();
    auto* registry = &CommandRegister::instance();
    if(conn->route_batches().empty()) conn->route_batches().resize(LoopsEngines->size());
    bool need_reply = conn->NeedReply();

    std::unordered_map<std::string_view, std::string> local_notwrite_cache;
    std::unordered_map<std::string_view, delay_write> local_last_write_slot;

    for(auto it = blocks.begin(); it != blocks.end();) {
        auto& buffer = *it;

        if(!conn->fragmented_buffer_.empty()) {
            std::cout << "fragment\n";
            conn->fragmented_buffer_.append(buffer->peek(), buffer->readableBytes());
            buffer->retrieveAll();

            std::size_t pos{0};
            std::vector<std::string_view> tokens;
            int status = this->try_parse_resp(conn->fragmented_buffer_.data(), conn->fragmented_buffer_.size(), pos, tokens);

            if(status == 1) {
                std::vector<std::string> deep_copied_tokens;
                deep_copied_tokens.reserve(tokens.size());
                for(auto& t : tokens) deep_copied_tokens.emplace_back(t);

                // 路由
                char cmd_buf[32];
                std::string_view raw_cmd = tokens[0];
                std::size_t cmd_len = std::min(raw_cmd.size(), (std::size_t)31);

                for(int i = 0; i < cmd_len; ++i) {
                    cmd_buf[i] = std::tolower(raw_cmd[i]);
                }

                std::string_view cmd_view(cmd_buf, cmd_len);

                const CommandDef* cmd_def = registry->lookup(cmd_view);

                uint64_t current_slot_id = 0;

                if(need_reply) {
                    ResponseSlot slot;
                    current_slot_id = conn->appendPendRes(slot);
                }

                if(!cmd_def) {
                    if(need_reply) conn->fillSingleSlot(current_slot_id, UNKNOWNCOMMANDSTR);

                    continue;
                }

                if((cmd_def->arity_ > 0 && tokens.size() != cmd_def->arity_)||
                    (cmd_def->arity_ < 0 && tokens.size() < -cmd_def->arity_)) {

                    if(need_reply) conn->fillSingleSlot(current_slot_id, WRONGNUMBERSTR);

                    continue;
                }

                dispatch_command(conn, tokens, cmd_def, current_slot_id, conn->fragmented_buffer_, need_reply, LoopsEngines, 
                local_notwrite_cache, local_last_write_slot);

                // conn->fragmented_buffer_.erase(0, pos);

            } else if(status == -1) {
                conn->getCurrentReplyBuffer()->appendStatic(PROTOCOLERRORSTR, ::strlen(PROTOCOLERRORSTR));
                conn->flushWriteBatch();

                return -1;

            } else {
                ++it;
            }

            continue;
        }

        while(buffer->readableBytes() > 0 && conn->fragmented_buffer_.empty()) {
            std::vector<std::string_view> tokens_view;

            size_t pos = 0;
            int status = this->try_parse_resp(buffer->peek(), buffer->readableBytes(), pos, tokens_view);

            if(status == 0) {
                auto next_it = std::next(it);
                if(next_it != blocks.end()) {
                    conn->fragmented_buffer_.append(buffer->peek(), buffer->readableBytes());
                    buffer->retrieveAll();
                    break;

                } else {
                    goto EXECUTE_LOCAL_WRITES;
                }
            }

            else if(status == -1) {
                conn->getCurrentReplyBuffer()->appendStatic(PROTOCOLERRORSTR, ::strlen(PROTOCOLERRORSTR));
                buffer->retrieveAll();
                conn->flushWriteBatch();

                return -1;
            }

            if(tokens_view.empty()) { buffer->retrieve(pos); continue; }

            char cmd_buf[32];
            std::string_view raw_cmd = tokens_view[0];
            std::size_t cmd_len = std::min(raw_cmd.size(), (std::size_t)31);

            for(int i = 0; i < cmd_len; ++i) {
                cmd_buf[i] = std::tolower(raw_cmd[i]);
            }

            std::string_view cmd_view(cmd_buf, cmd_len);

            const CommandDef* cmd_def = registry->lookup(cmd_view);

            bool need_reply = conn->NeedReply();
            uint64_t current_slot_id = 0;

            if(need_reply) {
                ResponseSlot slot;
                current_slot_id = conn->appendPendRes(slot);
            }

            if(!cmd_def) {
                if(need_reply) conn->fillSingleSlot(current_slot_id, UNKNOWNCOMMANDSTR);

                buffer->retrieve(pos);
                continue;
            }

            if((cmd_def->arity_ > 0 && tokens_view.size() != cmd_def->arity_)||
                (cmd_def->arity_ < 0 && tokens_view.size() < -cmd_def->arity_)) {

                if(need_reply) conn->fillSingleSlot(current_slot_id, WRONGNUMBERSTR);

                buffer->retrieve(pos);
                continue;
            }

            dispatch_command(conn, tokens_view, cmd_def, current_slot_id, buffer, need_reply, LoopsEngines, 
            local_notwrite_cache, local_last_write_slot);

            buffer->retrieve(pos);
        }

        if(buffer->readableBytes() == 0) ++it;
    }

EXECUTE_LOCAL_WRITES:
    for(auto it = local_last_write_slot.begin(); it != local_last_write_slot.end(); ++it) {
        auto& delay_write = it->second;

        CommandContext ctx{delay_write.tokens_, this->engine_, false, curr_loop, curr_loop, "", false};
        this->executeCommandAndPersist(ctx, delay_write.cmd_def_);

        for(auto& slot_id : delay_write.slot_indexs_) {
            std::string delay_res = ctx.response_;
            if(need_reply) conn->fillSingleSlot(slot_id, std::move(delay_res));
        }
    }

    conn->tryFlushResponses();

END_PARSE:
    for(int i = 0; i < LoopsEngines->size(); ++i) {
        auto& batch = conn->route_batches()[i];
        Ringengine* target_engine = (*LoopsEngines)[i].second;
        EventLoop* curr_loop = EventLoop::getEventLoopOfCurrentThread();

        if(batch.empty()) continue;

        EventLoop* target_loop = (*LoopsEngines)[i].first;

        AeroIO::net::RouteBatchTaskPtr task = RouteBatchTaskPool::getInstance().get(curr_loop);
        task->conn_ = conn;
        task->current_loop_ = curr_loop;
        task->target_engine_ = target_engine;
        task->target_loop_ = target_loop;
        task->cmd_count_ = batch.size();
        task->execute_ = KvsProtocolHandler::executeCommandAndPersist;
        
        for(int j = 0; j < batch.size(); ++j) {
            task->cmds[j] = std::move(batch[j]);
        }

        target_loop->runInLoop([task]() mutable {
            (*task)(); 
        });

        batch.clear();
    }

    conn->tryFlushResponses();

    return 0;
}

int KvsProtocolHandler::dispatch(const AeroIO::net::TcpConnectionPtr& conn, 
    std::vector<std::string_view>& tokens, ReplyBuffer* reply) {

    std::string cmd(tokens[0]);

    for(auto& c : cmd) c = toupper(c);

    // else if(cmd == "BGSAVE") {
    //     if(tokens.size() != 1) { conn->multi_responses.push_back(std::string("-ERR wrong number of arguments for 'bgsave' command\r\n")); return -1; }
    //     if(multimodel) {
    //         conn->multi_buf.emplace_back(std::move(tokens));
    //         conn->multi_responses.emplace_back("+QUEUED\r\n");
    //         return 0;
    //     }

    //     if(ctx.config->conf_persist.enable && ctx.config->conf_persist.persist_type == Persist_Config::Persist_Type::RDB) {
    //         conn->multi_responses.push_back(std::string("+OK\r\n"));
    //         ctx.pm->schedule_rdb_dump();

    //     } else { conn->multi_responses.push_back(std::string("-ERR internal error\r\n")); }

    //     return 0;
    // }

    // else if(cmd == "MULTI") {
    //     if(tokens.size() != 1) { conn->send("-ERR wrong number of arguments for 'multi' command\r\n"); return -1; }

    //     conn->multi_status = true;
    //     if(need_reply) conn->multi_responses.emplace_back("+OK\r\n");

    //     return 0;
    // }

    // else if(cmd == "EXEC") {
    //     if(tokens.size() != 1) { conn->multi_responses.push_back(std::string("-ERR wrong number of arguments for 'exec' command\r\n")); return -1; }
    //     if(!multimodel) { conn->multi_responses.push_back(std::string("-ERR EXEC without MULTI\r\n")); return -1; }

    //     conn->multi_status = false;
    //     conn->multi_responses.emplace_back("*" + std::to_string(conn->mulitcommandnum - 1) + "\r\n");

    //     for(auto& multokens : conn->multi_buf) {
    //         dispatch(kvs_server, multokens, conn);
    //     }

    //     conn->multi_buf.clear();
    //     conn->mulitcommandnum = 0;

    //     return 0;
    // }

    

    return 0;
}

};