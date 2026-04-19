#include "engine.h"
#include "kvstr.h"
#include "jemalloc.h"
#include "list.h"
#include "robject.h"
#include "zset.h"
#include "kvstr.h"
#include <limits>
#include <iostream>
#include <charconv>
#include <string.h>

namespace rkv {

JemallocWrapper* rdict::mempool() const 
{ return this->mempool_; }

rdict::rdict(JemallocWrapper* pool) : Ringengine(pool) {
    // this->dict_.getUnderlyingMap().reserve(6000000);
}

EngineMap& rdict::getUnderlyingMap()
{ return this->dict_.getUnderlyingMap(); }

int rdict::del(std::string_view key) {
    return this->dict_.hash_del(key);
}

void rdict::flushall() {
    this->dict_.flushall();
}

RedisObject* rdict::getMeta(std::string_view key) {
    auto opt_obj = this->dict_.hash_get(key);

    if(!opt_obj.has_value()) return nullptr;

    return opt_obj.value();
}

// String Command
bool rdict::exist(std::string_view key) {
    return this->dict_.hash_exists(key);
}

int rdict::set(std::string_view key, std::string_view value) {
    auto& map = this->dict_.getUnderlyingMap();
    auto it = map.find(key);

    if(it != map.end()) {
        RedisObject* obj = it->second;

        if(obj->type == OBJ_STRING) {
            if(obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;

                int capacity = s->len_ + s->free_;
                if(capacity >= value.size()) {
                    ::memcpy(s->buf_, value.data(), value.size());
                    s->buf_[value.size()] = '\0';
                    s->len_ = value.size();
                    s->free_ = capacity - s->len_;

                    return 0;
                }

                this->mempool_->free(s);
                s = kvstr::KvstrCreate(value.data(), value.size(), this->mempool_);
                if(s) {
                    obj->ptr = (void*)s;
                    obj->len = value.size();

                    return 0;

                } else {
                    return -1;
                }

            } else if(obj->encoding == OBJ_ENCODING_INT) {
                kvstr* s = kvstr::KvstrCreate(value.data(), value.size(), this->mempool_);
                if(s) {
                    obj->ptr = (void*)s;
                    obj->encoding = OBJ_ENCODING_RAW;
                    obj->len = value.size();

                    return 0;

                } else {
                    return -1;
                }
            }

        } else return -2;
    }

    RedisObject* obj = kvstr::CreateStringObject(value.data(), value.size(), this->mempool_);
    if(obj == nullptr) return -3;

    kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
    if(!new_key) return -4;
    this->dict_.emplace(new_key, obj);

    return 0;
}

int rdict::setnx(std::string_view key, std::string_view value) {
    auto it = this->dict_.find(key);

    if(it != this->dict_.end()) return 1;

    RedisObject* obj = kvstr::CreateRawStringObject(value.data(), value.size(), this->mempool_);
    if(obj == nullptr) return -1;

    kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
    if(!new_key) return -2;
    this->dict_.emplace(new_key, obj);

    return 0;
}

RedisObject* rdict::get(std::string_view key) {

    auto opt_obj = this->dict_.hash_get(key);

    if(!opt_obj.has_value()) return nullptr;

    RedisObject* obj = opt_obj.value();

    if(obj == nullptr || obj->type != OBJ_STRING) return nullptr;

    return obj;
}

int rdict::incr(std::string_view key, long long* result) {
    auto opt_obj = this->dict_.hash_get(key);
    long long val;

    if(!opt_obj.has_value()) {
        val = 1;
        RedisObject* newstringobj = kvstr::CreateIntStringObject(val, this->mempool_);
        if(newstringobj == nullptr) return KVS_ERR_NULL;

        kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
        if(!new_key) return KVS_ERR_NULL;

        this->dict_.emplace(new_key, newstringobj);

        if(result) *result = val;
        return KVS_OK;
    }

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_STRING) return KVS_ERR_TYPE; // ERR value is not an integer or out of range

    if(obj->encoding == OBJ_ENCODING_INT) val = (long long)obj->ptr;
    else if(obj->encoding == OBJ_ENCODING_RAW || obj->encoding == OBJ_ENCODING_EMBSTR) {
        kvstr* s = (kvstr*)obj->ptr;
        if(string2ll(s->buf_, s->len_, &val) == false) {
            return KVS_ERR_NOT_INT; // ERR value is not an integer or out of range
        }
        
    } else return KVS_ERR_NOT_INT; // Unknown encoding

    if(val > std::numeric_limits<long long>::max() - 1 || val < std::numeric_limits<long long>::min()) return KVS_ERR_OVERFLOW; // ERR increment or decrement would overflow
    ++val;

    if(obj->encoding == OBJ_ENCODING_RAW) {
        kvstr* s = (kvstr*)obj->ptr;

        this->mempool_->free(s);
        obj->encoding = OBJ_ENCODING_INT;

    } else if(obj->encoding == OBJ_ENCODING_EMBSTR) {
        obj->encoding = OBJ_ENCODING_INT;
    }

    obj->ptr = (void*)val;
    if(result) *result = val;

    return KVS_OK;
}

int rdict::incrby(std::string_view key, int increment, long long* result) {
    auto opt_obj = this->dict_.hash_get(key);
    long long val;

    if(!opt_obj.has_value()) {
        val = static_cast<long long>(increment);

        RedisObject* newstringobj = kvstr::CreateIntStringObject(val, this->mempool_);
        if(newstringobj == nullptr) return KVS_ERR_NULL;

        kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
        if(!new_key) return KVS_ERR_NULL;

        this->dict_.emplace(new_key, newstringobj);

        if(result) *result = val;
        return KVS_OK;
    }

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_STRING) return KVS_ERR_TYPE;

    if(obj->encoding == OBJ_ENCODING_INT) val = (long long)obj->ptr;
    else if(obj->encoding == OBJ_ENCODING_RAW || obj->encoding == OBJ_ENCODING_EMBSTR) {
        kvstr* s = (kvstr*)obj->ptr;
        if(string2ll(s->buf_, s->len_, &val) == false) return KVS_ERR_NOT_INT;

    } else return KVS_ERR_NOT_INT;

    if((increment > 0 && val > std::numeric_limits<long long>::max() - increment) || 
       (increment < 0 && val < std::numeric_limits<long long>::min() - increment)) return KVS_ERR_OVERFLOW;
    
    val += increment;

    if(obj->encoding == OBJ_ENCODING_RAW) {
        kvstr* s = (kvstr*)obj->ptr;

        this->mempool_->free(s);
        obj->encoding = OBJ_ENCODING_INT;

    } else if(obj->encoding == OBJ_ENCODING_EMBSTR) {
        obj->encoding = OBJ_ENCODING_INT;
    }

    obj->ptr = (void*)val;
    if(result) *result = val;

    return KVS_OK;
}

int rdict::decr(std::string_view key, long long* result) {
    auto opt_obj = this->dict_.hash_get(key);
    long long val;

    if(!opt_obj.has_value()) {
        val = -1;

        RedisObject* newstringobj = kvstr::CreateIntStringObject(val, this->mempool_);
        if(newstringobj == nullptr) return KVS_ERR_NULL;

        kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
        if(!new_key) return KVS_ERR_NULL;

        this->dict_.emplace(new_key, newstringobj);
        
        if(result) *result = val;
        return KVS_OK;
    }

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_STRING) return KVS_ERR_TYPE;

    if(obj->encoding == OBJ_ENCODING_INT) val = (long long)obj->ptr;
    else if(obj->encoding == OBJ_ENCODING_RAW || obj->encoding == OBJ_ENCODING_EMBSTR) {
        kvstr* s = (kvstr*)obj->ptr;

        if(string2ll(s->buf_, s->len_, &val) == false) return KVS_ERR_NOT_INT;

    } else return KVS_ERR_NOT_INT;

    if(val < std::numeric_limits<long long>::min() + 1 || val > std::numeric_limits<long long>::max()) return KVS_ERR_OVERFLOW;
    --val;

    if(obj->encoding == OBJ_ENCODING_RAW) {
        kvstr* s = (kvstr*)obj->ptr;
        size_t kvstrtotalsize = sizeof(kvstr) + s->len_ + 1;

        this->mempool_->free(s);
        obj->encoding = OBJ_ENCODING_INT;

    } else if(obj->encoding == OBJ_ENCODING_EMBSTR) {
        obj->encoding = OBJ_ENCODING_INT;
    }

    obj->ptr = (void*)val;
    if(result) *result = val;

    return KVS_OK;
}

int rdict::decrby(std::string_view key, int decrement, long long* result) {
    auto opt_obj = this->dict_.hash_get(key);
    long long val;

    if(!opt_obj.has_value()) {
        val = static_cast<long long>(-1 * decrement);

        RedisObject* newstringobj = kvstr::CreateIntStringObject(val, this->mempool_);
        if(newstringobj == nullptr) return KVS_ERR_NULL;

        kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
        if(!new_key) return KVS_ERR_NULL;

        this->dict_.emplace(new_key, newstringobj);

        if(result) *result = val;
        return KVS_OK;
    }

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_STRING) return KVS_ERR_TYPE;

    if(obj->encoding == OBJ_ENCODING_INT) val = (long long)obj->ptr;
    else if(obj->encoding == OBJ_ENCODING_RAW || obj->encoding == OBJ_ENCODING_EMBSTR) {
        kvstr* s = (kvstr*)obj->ptr;

        if(string2ll(s->buf_, s->len_, &val) == false) return KVS_ERR_NOT_INT;

    } else return KVS_ERR_NOT_INT;

    if((decrement > 0 && val < std::numeric_limits<long long>::min() + decrement) || 
       (decrement < 0 && val > std::numeric_limits<long long>::max() - decrement)) return KVS_ERR_OVERFLOW;
    val -= decrement;

    if(obj->encoding == OBJ_ENCODING_RAW) {
        kvstr* s = (kvstr*)obj->ptr;
        size_t kvstrtotalsize = sizeof(kvstr) + s->len_ + 1;

        this->mempool_->free(s);
        obj->encoding = OBJ_ENCODING_INT;

    } else if(obj->encoding == OBJ_ENCODING_EMBSTR) {
        obj->encoding = OBJ_ENCODING_INT;
    }

    obj->ptr = (void*)val;
    if(result) *result = val;

    return KVS_OK;
}

int rdict::lpush(std::string_view key, std::vector<std::string_view>& values) {
    auto LeftPushList = [this, &values] (RedisObject* obj) {
        ListObject* list = (ListObject*)obj->ptr;

        for(int i = 2; i < values.size(); ++i) {
            kvstr* s = kvstr::KvstrCreate(values[i].data(), values[i].size(), this->mempool_);
            list->list_push_head(s, this->mempool_);
        }

        return list->getlen();
    };

    auto opt_obj = this->dict_.hash_get(key);
    if(opt_obj.has_value()) {
        RedisObject* obj = opt_obj.value();
        if(obj->type == OBJ_LIST) return LeftPushList(obj);

        return -1;
    }

    RedisObject* newlistobj = ListObject::CreateListObject(this->mempool_);
    if(newlistobj == nullptr) return -2;

    kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
    if(!new_key) {
        robject::freeRedisObject(newlistobj, this->mempool_);
        return -3;
    }

    this->dict_.emplace(new_key, newlistobj);

    return LeftPushList(newlistobj);
}

int rdict::rpush(std::string_view key, std::vector<std::string_view>& values) {
    auto RightPushList = [this, &values] (RedisObject* obj) {
        ListObject* list = (ListObject*)obj->ptr;

        for(int i = 2; i < values.size(); ++i) {
            kvstr* s = kvstr::KvstrCreate(values[i].data(), values[i].size(), this->mempool_);
            list->list_push_tail(s, this->mempool_);
        }

        return list->getlen();
    };

    auto opt_obj = this->dict_.hash_get(key);
    if(opt_obj.has_value()) {
        RedisObject* obj = opt_obj.value();
        if(obj->type == OBJ_LIST) return RightPushList(obj);

        return -1;
    }

    RedisObject* newlistobj = ListObject::CreateListObject(this->mempool_);
    if(newlistobj == nullptr) return -2;

    kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
    if(!new_key) {
        robject::freeRedisObject(newlistobj, this->mempool_);
        return -3;
    }

    this->dict_.emplace(new_key, newlistobj);

    return RightPushList(newlistobj);
}

int rdict::lset(std::string_view key, int index, std::string_view value) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return -1;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_LIST) return -2;

    ListObject* list = (ListObject*)obj->ptr;

    return list->list_modify_index(index, value, this->mempool_);
}

std::vector<kvstr*> rdict::lpop(std::string_view key, std::size_t count) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return {};

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_LIST) return {};

    ListObject* list = (ListObject*)obj->ptr;
    std::vector<kvstr*> res;

    int pop_size = std::min(count, list->getlen());
    res.resize(pop_size);

    for(int i = 0; i < pop_size; ++i) {
        kvstr* obj = list->list_pop_head(this->mempool_);
        res[i] = obj;
    
        if(list->getlen() <= 0) this->dict_.hash_del(key);
    }

    return res;
}

std::vector<kvstr*> rdict::rpop(std::string_view key, std::size_t count) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return {};

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_LIST) return {};

    ListObject* list = (ListObject*)obj->ptr;
    std::vector<kvstr*> res;

    int pop_size = std::min(count, list->getlen());
    res.resize(pop_size);

    for(int i = 0; i < pop_size; ++i) {
        kvstr* obj = list->list_pop_tail(this->mempool_);
        res[i] = obj;

        if(list->getlen() <= 0) this->dict_.hash_del(key);
    }

    return res;
}

kvstr* rdict::lindex(std::string_view key, int index) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return nullptr;

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_LIST) return nullptr;

    ListObject* list = (ListObject*)obj->ptr;

    return list->list_index(index);
}

std::size_t rdict::llen(std::string_view key) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return 0;

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_LIST) return -1;

    ListObject* list = (ListObject*)obj->ptr;

    return list->getlen();
}

int rdict::hset(std::string_view key, std::vector<std::string_view>& values) {
    if(values.size() % 2 != 0) return -1;

    auto opt_obj = this->dict_.hash_get(key);
    if(opt_obj.has_value()) {
        RedisObject* obj = opt_obj.value();
        if(obj->type != OBJ_HASH) return -2;

        HashObject* hash = (HashObject*)obj->ptr;


    }
}

bool rdict::hget(std::string_view, std::string_view) {

}

int rdict::hdel(std::string_view, std::vector<std::string_view>&) {

}

bool rdict::hexists(std::string_view, std::string&) {

}

std::size_t rdict::hlen(std::string_view) {

}

std::vector<kvstr*> rdict::hgetall(std::string_view) {
    
}

std::vector<kvstr*> rdict::lrange(std::string_view key, int start, int stop) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return {};

    RedisObject* obj = opt_obj.value();

    if(obj->type != OBJ_LIST) return {};

    ListObject* list = (ListObject*)obj->ptr;

    return list->list_range(start, stop);
}

int rdict::zadd(std::string_view key, std::vector<std::string_view>& values) {
    auto add_to_zset = [&values, this] (RedisObject* obj) -> int {
        ZSetObject* zobj = (ZSetObject*)obj->ptr;
        int res{0}, val_size = values.size();

        int index{3};
        while(index < val_size) {
            double socre{0};
            const char* start = values[index - 1].data();
            const char* end = values[index - 1].data() + values[index - 1].size();
            auto [p, ec] = std::from_chars(start, end, socre);
            if(ec != std::errc() || res < 0) return -1;

            res += zobj->zset_add(socre, values[index], this->mempool_);
            index += 2;
        }

        return res;
    };

    auto opt_obj = this->dict_.hash_get(key);
    if(opt_obj.has_value()) {
        RedisObject* obj = opt_obj.value();
        if(obj->type != OBJ_ZSET) return -2;

        return add_to_zset(obj);
    }

    RedisObject* newzsetobj = ZSetObject::CreateZSetObject(this->mempool_);
    if(newzsetobj == nullptr) return -3;

    kvstr* new_key = kvstr::KvstrCreate(key.data(), key.size(), this->mempool_);
    if(!new_key) {
        robject::freeRedisObject(newzsetobj, this->mempool_);
        return -3;
    }

    this->dict_.emplace(new_key, newzsetobj);

    return add_to_zset(newzsetobj);
}

std::vector<kvstr*> rdict::zrevrange(std::string_view key, int start, int stop) {
    std::vector<kvstr*> result{};

    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return result;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return result;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    
    return zset->zset_revrange(start, stop);
}

std::vector<kvstr*> rdict::zrange(std::string_view key, int start, int stop) {
    std::vector<kvstr*> result{};

    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return result;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return result;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    
    return zset->zset_range(start, stop);
}

std::optional<double> rdict::zscore(std::string_view key, std::string_view member) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return std::nullopt;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return std::nullopt;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    auto opt_score = zset->zset_score(member);
    if(!opt_score.has_value()) return std::nullopt;

    return opt_score.value();
}

int rdict::zcard(std::string_view key) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return 0;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return 0;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    return zset->zset_card();
}

std::optional<int> rdict::zrank(std::string_view key, std::string_view member) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return std::nullopt;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return std::nullopt;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    return zset->zset_rank(member);
}

std::optional<int> rdict::zrevrank(std::string_view key, std::string_view member) {
    auto opt_obj = this->dict_.hash_get(key);
    if(!opt_obj.has_value()) return std::nullopt;

    RedisObject* obj = opt_obj.value();
    if(obj->type != OBJ_ZSET) return std::nullopt;

    ZSetObject* zset = (ZSetObject*)obj->ptr;
    return zset->zset_revrank(member);
}

};