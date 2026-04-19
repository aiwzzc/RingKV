#include "CommandHandlers.h"
#include "engine.h"
#include "common.h"
#include "kvstr.h"
#include "respresstr.h"
#include "server.h"
#include "list.h"
#include "zset.h"
#include "AeroIO/net/TcpConnection.h"
#include "AeroIO/net/EventLoop.h"
#include <span>
#include <iostream>
#include <charconv>

namespace {

    uint64_t getCurrentTimestamp() {
        auto now = std::chrono::system_clock::time_point::clock::now();
        auto duration = now.time_since_epoch();
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        return milliseconds.count(); //单位是毫秒
    }

    bool isDigit(std::string_view val) {
        for(auto& c : val) {
            if(!std::isdigit(c)) return false;
        }

        return true;
    }

    std::optional<int> string2int(std::string_view val) {
        int res{0};
        bool isNegative{false};

        const char* start = val.data();
        const char* end = val.data() + val.size();

        if(*start == '-') {
            ++start;
            isNegative = true;
        }

        auto [p, ec] = std::from_chars(start, end, res);
        if(ec != std::errc() || res < 0) return std::nullopt;

        return isNegative ? -res : res;
    }

};

namespace rkv {

CommandRegister::CommandRegister() {
    registerCmd({"command", 2, false, false, 0, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::commandCommand});
    registerCmd({"ping", 1, false, false, 0, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::pingCommand});
    registerCmd({"bgsave", 1, false, false, 0, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::bgsaveCommand});
    registerCmd({"info", 1, false, false, 0, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::infoCommand});
    registerCmd({"expire", 3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::expireCommand});
    registerCmd({"flushall", 1, true, false, 0, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::flushallCommand});
    registerCmd({"keys", 2, false, true, 1, KEYTYPE::GLOBAL_KEY});

    registerCmd({"exists", -2, false, true, 1, KEYTYPE::MULTI_KEY, 1});
    registerCmd({"del", -2, true, true, 1, KEYTYPE::MULTI_KEY, 1});
    registerCmd({"mget", -2, false, true, 1, KEYTYPE::MULTI_KEY, 1});
    registerCmd({"set", -3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::setCommand});
    registerCmd({"setnx", 3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::setnxCommand});
    registerCmd({"get", 2, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::getCommand});
    registerCmd({"incr", 2, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::incrCommand});
    registerCmd({"incrby", 3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::incrbyCommand});
    registerCmd({"decr", 2, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::decrCommand});
    registerCmd({"decrby", 3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::decrbyCommand});
    
    registerCmd({"lpush", -3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::lpushCommand});
    registerCmd({"rpush", -3, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::rpushCommand});
    registerCmd({"lpop", -2, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::lpopCommand});
    registerCmd({"rpop", -2, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::rpopCommand});
    registerCmd({"llen", 2, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::llenCommand});
    registerCmd({"lset", 4, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::lsetCommand});
    registerCmd({"lindex", 3, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::lindexCommand});
    registerCmd({"lrange", 4, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::lrangeCommand});

    registerCmd({"zadd", -4, true, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zaddCommand});
    registerCmd({"zrevrange", 4, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zrevrangeCommand});
    registerCmd({"zrange", 4, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zrangeCommand});
    registerCmd({"zscore", 3, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zscoreCommand});
    registerCmd({"zcard", 2, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zcardCommand});
    registerCmd({"zrank", 3, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zrankCommand});
    registerCmd({"zrevrank", 3, false, true, 1, KEYTYPE::SINGLE_KEY, 0, CommandHandlers::zrevrankCommand});
}

void CommandRegister::registerCmd(CommandDef def) {
    this->cmd_table_.getUnderlyingMap()[def.name_] = def;
}

const CommandDef* CommandRegister::lookup(std::string_view name) {
    auto it = this->cmd_table_.getUnderlyingMap().find(name);

    return it != this->cmd_table_.getUnderlyingMap().end() ? &it->second : nullptr;
}

CommandRegister& CommandRegister::instance() {
    static CommandRegister registry;

    return registry;
}

MultiKeyCoordinator::MultiKeyCoordinator(AeroIO::net::TcpConnectionPtr conn, int slot_id, const std::string& cmd_name, int total_keys):
    conn_(conn), slot_id_(slot_id), cmd_name_(cmd_name), mget_results_(total_keys) {

    this->handle_["del"] = CommandHandlers::delCommand;
    this->handle_["exists"] = CommandHandlers::existsCommand;
    this->handle_["mget"] = CommandHandlers::mgetCommand;
}

void MultiKeyCoordinator::onComplete() {
    if(this->pending_tasks_.fetch_sub(1, std::memory_order_relaxed) == 1) {
        this->finalizeResponse();
    }
}

void MultiKeyCoordinator::finalizeResponse() {
    std::string finish_str;

    if(this->cmd_name_ == "del") {
        finish_str = ":" + std::to_string(this->delete_count_.load(std::memory_order_relaxed)) + "\r\n";

    } else if (this->cmd_name_ == "exists") {
        finish_str = ":" + std::to_string(this->exists_count_.load(std::memory_order_relaxed)) + "\r\n";

    } else if (this->cmd_name_ == "mget") {
        finish_str = "*" + std::to_string(this->mget_results_.size()) + "\r\n";
        for(auto& res : this->mget_results_) {
            finish_str += res;
        }
    }

    this->conn_->getLoop()->runInLoop([finish_str = std::move(finish_str), this] () mutable {
        this->conn_->fillSingleSlot(this->slot_id_, std::move(finish_str));
        this->conn_->tryFlushResponses();
    });
}

GlobalKeyCoordinator::GlobalKeyCoordinator(AeroIO::net::TcpConnectionPtr conn, int slot_id, const std::string& cmd_name):
    conn_(conn), slot_id_(slot_id), cmd_name_(cmd_name) {
        this->handle_["keys"] = CommandHandlers::keysCommand;
    }

void GlobalKeyCoordinator::onComplete() {
    if(this->pending_tasks_.fetch_sub(1, std::memory_order_relaxed) == 1) {
        this->finalizeResponse();
    }
}

void GlobalKeyCoordinator::finalizeResponse() {
    std::string finish_str;

    if(this->cmd_name_ == "keys") {
        finish_str = "*" + std::to_string(this->keys_results_.size()) + "\r\n";
        for(auto& [engine, keys] : this->keys_results_) {
            for(auto& key : keys) {
                finish_str += key;
            }
        }
    }

    this->conn_->getLoop()->runInLoop([this, finish_str = std::move(finish_str)] () mutable {
        this->conn_->fillSingleSlot(this->slot_id_, std::move(finish_str));
        this->conn_->tryFlushResponses();
    });
}

void CommandHandlers::commandCommand(CommandContext& ctx) {
    ctx.success_ = true;
    ctx.response_ = EMPTYARRAYSTR;
}

void CommandHandlers::pingCommand(CommandContext& ctx) {
    ctx.success_ = true;
    ctx.response_ = PONGSTR;
}

bool CommandHandlers::timeout_delete(std::string_view key, Ringengine* engine) {
    auto it = kvserver::expires_.getUnderlyingMap().find(key);
    if(it != kvserver::expires_.getUnderlyingMap().end() && getCurrentTimestamp() >= it->second) {
        engine->del(key);
        kvserver::expires_.getUnderlyingMap().erase(it);
        return true;
    }

    return false;
}

bool CommandHandlers::timeout_delete_expire(std::string_view key) {
    auto it = kvserver::expires_.getUnderlyingMap().find(key);
    if(it != kvserver::expires_.getUnderlyingMap().end() && getCurrentTimestamp() >= it->second) {
        kvserver::expires_.getUnderlyingMap().erase(it);
        return true;
    }

    return false;
}

void CommandHandlers::timeout_delete(std::string_view key) {
    auto it = kvserver::expires_.getUnderlyingMap().find(key);
    kvserver::expires_.getUnderlyingMap().erase(it);
}

void CommandHandlers::timeout_exist_delete(std::string_view key) {
    auto it = kvserver::expires_.getUnderlyingMap().find(key);

    if(it != kvserver::expires_.getUnderlyingMap().end()) {
        kvserver::expires_.getUnderlyingMap().erase(it);
    }
}

bool CommandHandlers::timeout_exist(std::string_view key) {
    auto it = kvserver::expires_.getUnderlyingMap().find(key);

    return it != kvserver::expires_.getUnderlyingMap().end();
}

void CommandHandlers::flushallCommand(CommandContext& ctx) {
    auto* LoopsEngines = ctx.curr_loop_->getLoopsEngines();

    for(auto& [loop, engine] : (*LoopsEngines)) {
        if(loop == ctx.curr_loop_) continue;

        loop->runInLoop([engine] () {
            kvserver::expires_.getUnderlyingMap().clear();
            engine->flushall();
        });
    }

    kvserver::expires_.getUnderlyingMap().clear();
    ctx.engine_->flushall();

    ctx.response_ = OKSTR;
}

void CommandHandlers::expireCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view value = ctx.tokens_[2];
    int result{0};

    if(!isDigit(value)) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    ctx.success_ = true;

    if(ctx.engine_->exist(key)) {
        result = 1;

        auto opt_time = string2int(value);
        int time = opt_time.value();

        kvserver::expires_.getUnderlyingMap()[std::string(key)] = getCurrentTimestamp() + time * 1000;
    }

    ctx.response_ = ":" + std::to_string(result) + "\r\n";
}

void CommandHandlers::bgsaveCommand(CommandContext& ctx) {

}

void CommandHandlers::infoCommand(CommandContext& ctx) {

}

void CommandHandlers::setCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view value = ctx.tokens_[2];

    timeout_exist_delete(key);

    int ret = ctx.engine_->set(key, value);
    if(ret >= 0) {
        ctx.success_ = true;
        ctx.response_ = OKSTR;

    } else {
        ctx.success_ = false;
        ctx.response_ = INTERNALERRSTR;
    }
}

void CommandHandlers::setnxCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view value = ctx.tokens_[2];

    if(timeout_delete_expire(key)) {
        int ret = ctx.engine_->set(key, value);
        if(ret >= 0) {
            ctx.success_ = true;
            ctx.response_ = ONESTR;

        } else {
            ctx.success_ = false;
            ctx.response_ = INTERNALERRSTR;
        }
        
        return;
    }

    int ret = ctx.engine_->setnx(key, value);
    ctx.success_ = true;

    if(ret == 0) {
        ctx.response_ = ONESTR;

    } else {
        ctx.response_ = ZEROSTR;
    }
}

void CommandHandlers::getCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    ctx.success_ = true;

    if(timeout_delete(key, ctx.engine_)) {
        ctx.response_ = STRINGERRSTR;
        return;
    }

    RedisObject* obj = ctx.engine_->get(key);
    if(obj && obj->type == OBJ_STRING) {
        if(obj->encoding == OBJ_ENCODING_RAW || obj->encoding == OBJ_ENCODING_EMBSTR) {
            kvstr* s = (kvstr*)obj->ptr;

            ctx.response_ = "$" + std::to_string(s->len_) + "\r\n" + 
            std::string(s->buf_, s->len_) + "\r\n";

        } else if (obj->encoding == OBJ_ENCODING_INT) {
            std::string digit_str = std::to_string((long long)obj->ptr);
            ctx.response_ = "$" + std::to_string(digit_str.size()) + "\r\n" +
            digit_str + "\r\n";
        }

    } else {
        ctx.response_ = STRINGERRSTR;
    }
}

void CommandHandlers::keysCommand(GlobalSubTask& task, Ringengine* engine, GlobalKeyCoordinator::GlobalCoordinatorPtr coordinator) {
#if 0
    std::vector<std::string_view>& tokens = task.tokens_;

    if(tokens[1] == "*") {
        auto& map = engine->getUnderlyingMap();

        int bucket_cursor{0}, total_bucket = map.bucket_count();

        while(bucket_cursor < total_bucket) {
            for(auto it = map.begin(bucket_cursor); it != map.end(bucket_cursor); ++it) {
                if(timeout_delete(it->first, engine)) continue;

                coordinator->keys_results_[engine].push_back("$" + std::to_string(it->first.size()) + "\r\n" +
                it->first + "\r\n");
            }

            ++bucket_cursor;
        }
    }

    coordinator->onComplete();
#endif
}


void CommandHandlers::existsCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator) {
    std::vector<std::string_view>& keys = task.keys_;
    int exists_success_count{0};
    
    for(auto& key : keys) {
        if(timeout_delete(key, engine)) continue;

        if(engine->exist(key)) ++exists_success_count;
    }

    coordinator->exists_count_.fetch_add(exists_success_count, std::memory_order_relaxed);
    coordinator->onComplete();
}

void CommandHandlers::delCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator) {
    std::vector<std::string_view>& keys = task.keys_;

    int del_success_count{0};
    for(auto& key : keys) {
        if(timeout_delete(key, engine)) continue;

        del_success_count += engine->del(key);
    }

    coordinator->delete_count_.fetch_add(del_success_count, std::memory_order_relaxed);
    coordinator->onComplete();
}

void CommandHandlers::mgetCommand(SubTask& task, Ringengine* engine, MultiKeyCoordinator::MultiCoordinatorPtr coordinator) {
    std::vector<std::string_view>& keys = task.keys_;
    const std::vector<int>& original_key_index = task.original_key_index_;

    for(int i = 0; i < keys.size(); ++i) {
        if(timeout_delete(keys[i], engine)) {
            coordinator->mget_results_[original_key_index[i]] = STRINGERRSTR;
            continue;
        }

        RedisObject* obj = engine->get(keys[i]);
        if(obj && obj->type == OBJ_STRING) {
            kvstr* s = (kvstr*)obj->ptr;

            coordinator->mget_results_[original_key_index[i]] = "$" + std::to_string(s->len_) + "\r\n" + 
            std::string(s->buf_, s->len_) + "\r\n";

        } else {
            coordinator->mget_results_[original_key_index[i]] = STRINGERRSTR;
        }
    }

    coordinator->onComplete();
}

void CommandHandlers::incrCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    long long newVal{0};

    if(timeout_delete_expire(key)) {
        newVal = 1;

        RedisObject* obj = ctx.engine_->get(key);
        if(obj && obj->type == OBJ_STRING) {
            if(obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
                if(isDigit(std::string(s->buf_, s->len_))) {
                    obj->encoding = OBJ_ENCODING_INT;
                    obj->ptr = (void*)newVal;
                    ctx.success_ = true;
                    ctx.response_ = ONESTR;

                } else {
                    ctx.success_ = false;
                    ctx.response_ = NOTINTOROUTRANGEERRSTR;
                }

            } else if (obj->encoding == OBJ_ENCODING_INT) {
                obj->ptr = (void*)newVal;
                ctx.success_ = true;
                ctx.response_ = ONESTR;
            }

        } else {
            ctx.success_ = false;
            ctx.response_ = NOTINTOROUTRANGEERRSTR;
        }

        return;
    }

    int ret = ctx.engine_->incr(key, &newVal);

    if(ret == KVS_OK) {
        ctx.response_ = ":" + std::to_string(newVal) + "\r\n";
        ctx.success_ = true;

    } else if(ret == KVS_ERR_NOT_INT) {
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        ctx.success_ = false;

    } else if(ret == KVS_ERR_OVERFLOW) {
        ctx.response_ = OVERFLOWERRSTR;
        ctx.success_ = false;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::incrbyCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view increment_str = ctx.tokens_[2];
    long long newVal{0};

    int increment;
    auto [p, ec] = std::from_chars(increment_str.data(), increment_str.data() + increment_str.size(), increment);
    if(ec != std::errc() || increment < 0) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    if(timeout_delete_expire(key)) {
        newVal = increment;

        RedisObject* obj = ctx.engine_->get(key);
        if(obj && obj->type == OBJ_STRING) {
            if(obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
                if(isDigit(std::string(s->buf_, s->len_))) {
                    obj->encoding = OBJ_ENCODING_INT;
                    obj->ptr = (void*)newVal;
                    ctx.success_ = true;
                    ctx.response_ = ":" + std::string(increment_str) + "\r\n";

                } else {
                    ctx.success_ = false;
                    ctx.response_ = NOTINTOROUTRANGEERRSTR;
                }

            } else if (obj->encoding == OBJ_ENCODING_INT) {
                obj->ptr = (void*)newVal;
                ctx.success_ = true;
                ctx.response_ = ONESTR;
            }

        } else {
            ctx.success_ = false;
            ctx.response_ = NOTINTOROUTRANGEERRSTR;
        }

        return;
    }

    int ret = ctx.engine_->incrby(key, increment, &newVal);

    if(ret == KVS_OK) {
        ctx.response_ = ":" + std::to_string(newVal) + "\r\n";
        ctx.success_ = true;

    } else if(ret == KVS_ERR_NOT_INT) {
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        ctx.success_ = false;

    } else if(ret == KVS_ERR_OVERFLOW) {
        ctx.response_ = OVERFLOWERRSTR;
        ctx.success_ = false;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::decrCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    long long newVal{0};

    if(timeout_delete_expire(key)) {
        newVal = -1;

        RedisObject* obj = ctx.engine_->get(key);
        if(obj && obj->type == OBJ_STRING) {
            if(obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
                if(isDigit(std::string(s->buf_, s->len_))) {
                    obj->encoding = OBJ_ENCODING_INT;
                    obj->ptr = (void*)newVal;
                    ctx.success_ = true;
                    ctx.response_ = ONESTR;

                } else {
                    ctx.success_ = false;
                    ctx.response_ = NOTINTOROUTRANGEERRSTR;
                }

            } else if (obj->encoding == OBJ_ENCODING_INT) {
                obj->ptr = (void*)newVal;
                ctx.success_ = true;
                ctx.response_ = ONESTR;
            }

        } else {
            ctx.success_ = false;
            ctx.response_ = NOTINTOROUTRANGEERRSTR;
        }

        return;
    }

    int ret = ctx.engine_->decr(key, &newVal);

    if(ret == KVS_OK) {
        ctx.response_ = ":" + std::to_string(newVal) + "\r\n";
        ctx.success_ = true;

    } else if(ret == KVS_ERR_NOT_INT) {
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        ctx.success_ = false;

    } else if(ret == KVS_ERR_OVERFLOW) {
        ctx.response_ = OVERFLOWERRSTR;
        ctx.success_ = false;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }

}

void CommandHandlers::decrbyCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view decrement_str = ctx.tokens_[2];
    long long newVal{0};

    int decrement;
    auto [p, ec] = std::from_chars(decrement_str.data(), decrement_str.data() + decrement_str.size(), decrement);
    if(ec != std::errc() || decrement < 0) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    if(timeout_delete_expire(key)) {
        newVal = decrement;

        RedisObject* obj = ctx.engine_->get(key);
        if(obj && obj->type == OBJ_STRING) {
            if(obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
                if(isDigit(std::string(s->buf_, s->len_))) {
                    obj->encoding = OBJ_ENCODING_INT;
                    obj->ptr = (void*)newVal;
                    ctx.success_ = true;
                    ctx.response_ = ":" + std::string(decrement_str) + "\r\n";

                } else {
                    ctx.success_ = false;
                    ctx.response_ = NOTINTOROUTRANGEERRSTR;
                }

            } else if (obj->encoding == OBJ_ENCODING_INT) {
                obj->ptr = (void*)newVal;
                ctx.success_ = true;
                ctx.response_ = ONESTR;
            }

        } else {
            ctx.success_ = false;
            ctx.response_ = NOTINTOROUTRANGEERRSTR;
        }

        return;
    }

    int ret = ctx.engine_->decrby(key, decrement, &newVal);

    if(ret == KVS_OK) {
        ctx.response_ = ":" + std::to_string(newVal) + "\r\n";
        ctx.success_ = true;

    } else if(ret == KVS_ERR_NOT_INT) {
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        ctx.success_ = false;

    } else if(ret == KVS_ERR_OVERFLOW) {
        ctx.response_ = OVERFLOWERRSTR;
        ctx.success_ = false;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::lpushCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::vector<std::string_view>& values = ctx.tokens_;

    timeout_delete(key, ctx.engine_);

    int ret = ctx.engine_->lpush(key, values);

    if(ret >= 0) {
        ctx.response_ = ":" + std::to_string(ret) + "\r\n";
        ctx.success_ = true;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::rpushCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::vector<std::string_view>& values = ctx.tokens_;

    timeout_delete(key, ctx.engine_);

    int ret = ctx.engine_->rpush(key, values);

    if(ret >= 0) {
        ctx.response_ = ":" + std::to_string(ret) + "\r\n";
        ctx.success_ = true;

    } else {
        ctx.response_ = INTERNALERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::lpopCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    int pop_count{0};

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    if(ctx.tokens_.size() == 3) {
        std::string_view count = ctx.tokens_[2];

        auto [p, ec] = std::from_chars(count.data(), count.data() + count.size(), pop_count);
        if(ec != std::errc() || pop_count < 0)  {
            ctx.success_ = false;
            ctx.response_ = POSITIVEERRORSTR;
            return;
        }

    } else pop_count = 1;

    std::vector<kvstr*> res = ctx.engine_->lpop(key, pop_count);
    if(res.empty()) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    ctx.response_ = "*" + std::to_string(res.size()) + "\r\n";

    for(auto* obj : res) {
        ctx.response_ += "$" + std::to_string(obj->len_) + "\r\n" +
        std::string(obj->buf_, obj->len_) + "\r\n";
    }
}

void CommandHandlers::rpopCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    int pop_count{0};

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    if(ctx.tokens_.size() == 3) {
        std::string_view count = ctx.tokens_[2];

        auto [p, ec] = std::from_chars(count.data(), count.data() + count.size(), pop_count);
        if(ec != std::errc() || pop_count < 0)  {
            ctx.success_ = false;
            ctx.response_ = POSITIVEERRORSTR;
            return;
        }

    } else pop_count = 1;

    std::vector<kvstr*> res = ctx.engine_->rpop(key, pop_count);
    if(res.empty()) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    ctx.response_ = "*" + std::to_string(res.size()) + "\r\n";

    for(auto* obj : res) {
        ctx.response_ += "$" + std::to_string(obj->len_) + "\r\n" +
        std::string(obj->buf_, obj->len_) + "\r\n";
    }
}

void CommandHandlers::lsetCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view index_str = ctx.tokens_[2];
    std::string_view element = ctx.tokens_[3];
    int index{0};

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = false;
        ctx.response_ = NOSUCHKEYERRSTR;
        return;
    }
    
    auto opt_index = string2int(index_str);

    if(!opt_index.has_value()) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    index = opt_index.value();

    int ret = ctx.engine_->lset(key, index, element);

    if(ret > 0) {
        ctx.response_ = OKSTR;
        ctx.success_ = true;

    } else if(ret == -1 || ret == -2) {
        ctx.response_ = NOSUCHKEYERRSTR;
        ctx.success_ = false;

    } else if(ret == -3) {
        ctx.response_ = INDEXOUTRANGEERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::lindexCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view index_str = ctx.tokens_[2];
    int index{0};

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    auto opt_index = string2int(index_str);

    if(!opt_index.has_value()) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    index = opt_index.value();

    kvstr* obj = ctx.engine_->lindex(key, index);

    if(obj == nullptr) ctx.response_ = STRINGERRSTR;

    ctx.response_ = "$" + std::to_string(obj->len_) + "\r\n" + 
    std::string(obj->buf_, obj->len_) + "\r\n";
}

void CommandHandlers::llenCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = ZEROSTR;
        return;
    }

    int ret = ctx.engine_->llen(key);

    if(ret >= 0) {
        ctx.response_ = ":" + std::to_string(ret) + "\r\n";
        ctx.success_ = true;

    } else {
        ctx.response_ = USEWRONGTYPEERRSTR;
        ctx.success_ = false;
    }
}

void CommandHandlers::lrangeCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view start_str = ctx.tokens_[2];
    std::string_view stop_str = ctx.tokens_[3];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    auto opt_start = string2int(start_str);
    auto opt_stop = string2int(stop_str);

    if(!opt_start.has_value() || !opt_stop.has_value()) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    int start = opt_start.value();
    int stop = opt_stop.value();

    std::vector<kvstr*> res = ctx.engine_->lrange(key, start, stop);
    if(res.empty()) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    ctx.response_ = "*" + std::to_string(res.size()) + "\r\n";

    for(auto* obj : res) {
        ctx.response_ += "$" + std::to_string(obj->len_) + "\r\n" +
        std::string(obj->buf_, obj->len_) + "\r\n";
    }
}

void CommandHandlers::zaddCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::vector<std::string_view>& values = ctx.tokens_;

    auto zadd_to_zset = [&ctx, &values, &key] () {
        if(values.size() % 2 != 0) {
            ctx.success_ = false;
            ctx.response_ = SYNTAXERRSTR;
            return;
        }

        int ret = ctx.engine_->zadd(key, values);

        if(ret >= 0) {
            ctx.success_ = true;
            ctx.response_ = ":" + std::to_string(ret) + "\r\n";

        } else if(ret == -1) {
            ctx.success_ = false;
            ctx.response_ = VALUENOTDOUBLE;

        } else if(ret == -2) {
            ctx.success_ = false;
            ctx.response_ = USEWRONGTYPEERRSTR;

        } else {
            ctx.success_ = false;
            ctx.response_ = UNKNOWNERRORSTR;
        }
    };

    if(timeout_delete_expire(key)) {
        RedisObject* obj = ctx.engine_->getMeta(key);
        if(obj->type != OBJ_ZSET) {
            ctx.success_ = false;
            ctx.response_ = USEWRONGTYPEERRSTR;
            return;
        }

        ZSetObject* zset = (ZSetObject*)obj->ptr;
        zset->release_all_node(ctx.engine_->mempool());

        zadd_to_zset();

        return;
    }

    zadd_to_zset();
}

void CommandHandlers::zrevrangeCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view start_str = ctx.tokens_[2];
    std::string_view stop_str = ctx.tokens_[3];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    auto opt_start = string2int(start_str);
    auto opt_stop = string2int(stop_str);

    if(!opt_start.has_value() || !opt_stop.has_value()) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    int start = opt_start.value();
    int stop = opt_stop.value();

    ctx.success_ = true;
    std::vector<kvstr*> res = ctx.engine_->zrevrange(key, start, stop);
    if(res.size() == 0) {
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    ctx.response_ = "*" + std::to_string(res.size()) + "\r\n";
    for(kvstr* s : res) {
        ctx.response_ += "$" + std::to_string(s->len_) + "\r\n" +
        std::string(s->buf_, s->len_) + "\r\n";
    }
}

void CommandHandlers::zrangeCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view start_str = ctx.tokens_[2];
    std::string_view stop_str = ctx.tokens_[3];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    auto opt_start = string2int(start_str);
    auto opt_stop = string2int(stop_str);

    if(!opt_start.has_value() || !opt_stop.has_value()) {
        ctx.success_ = false;
        ctx.response_ = NOTINTOROUTRANGEERRSTR;
        return;
    }

    int start = opt_start.value();
    int stop = opt_stop.value();

    ctx.success_ = true;
    std::vector<kvstr*> res = ctx.engine_->zrange(key, start, stop);
    if(res.size() == 0) {
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    ctx.response_ = "*" + std::to_string(res.size()) + "\r\n";
    for(kvstr* s : res) {
        ctx.response_ += "$" + std::to_string(s->len_) + "\r\n" +
        std::string(s->buf_, s->len_) + "\r\n";
    }
}

void CommandHandlers::zscoreCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view member = ctx.tokens_[2];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    auto opt_score = ctx.engine_->zscore(key, member);
    if(!opt_score.has_value()) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    ctx.success_ = true;
    ctx.response_ = ":" + std::to_string(opt_score.value()) + "\r\n";
}

void CommandHandlers::zcardCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = STRINGERRSTR;
        return;
    }

    int ret = ctx.engine_->zcard(key);
    ctx.success_ = true;
    ctx.response_ = ":" + std::to_string(ret) + "\r\n";
}

void CommandHandlers::zrankCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view member = ctx.tokens_[2];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    auto opt_score = ctx.engine_->zrank(key, member);
    if(!opt_score.has_value()) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    ctx.success_ = true;
    ctx.response_ = ":" + std::to_string(opt_score.value()) + "\r\n";
}

void CommandHandlers::zrevrankCommand(CommandContext& ctx) {
    std::string_view key = ctx.tokens_[1];
    std::string_view member = ctx.tokens_[2];

    if(timeout_delete(key, ctx.engine_)) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    auto opt_score = ctx.engine_->zrevrank(key, member);
    if(!opt_score.has_value()) {
        ctx.success_ = true;
        ctx.response_ = EMPTYARRAYSTR;
        return;
    }

    ctx.success_ = true;
    ctx.response_ = ":" + std::to_string(opt_score.value()) + "\r\n";
}
    
};