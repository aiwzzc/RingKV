#include "zset.h"
#include "jemalloc.h"
#include "common.h"

namespace rkv {

RedisObject* ZSetObject::CreateZSetObject(JemallocWrapper* mempool) {
    if(mempool == nullptr) return nullptr;

    RedisObject* obj = (RedisObject*)mempool->alloc(sizeof(RedisObject));
    if(obj == nullptr) return nullptr;

    obj->len = 0;
    obj->type = OBJ_ZSET;
    obj->refcount = 1;

    void* p = mempool->alloc(sizeof(ZSetObject));
    if(p == nullptr) return nullptr;

    ZSetObject* zobj = new (p) ZSetObject();
    obj->ptr = (void*)zobj;

    return obj;
}

void ZSetObject::release_all_node(JemallocWrapper* mempool) {
    if(mempool == nullptr || this->tree_.size() == 0) return;

    this->dict_.flushall();
    for(auto it = this->tree_.begin(); it != this->tree_.end();) {
        mempool->free((*it)->member_);
        mempool->free(*it);

        it = this->tree_.erase(it);
    }
}

int ZSetObject::zset_add(double score, std::string_view member, JemallocWrapper* mempool) {
    ZNode* node = nullptr;
    auto it = this->dict_.getUnderlyingMap().find(member);

    if(it != this->dict_.getUnderlyingMap().end()) {
        if(it->second->socre_ == score) return 0;

        node = it->second;
        this->tree_.erase(it->second);
        node->socre_ = score;

    } else {
        std::size_t total_len = sizeof(ZNode) + sizeof(kvstr) + member.size() + 1;
        void* mem = mempool->alloc(total_len);
        if(mem == nullptr) return -1;

        kvstr* s = (kvstr*)((char*)mem + sizeof(ZNode));
        s->len_ = member.size();
        s->free_ = 0;
        ::memcpy(s->buf_, member.data(), member.size());
        s->buf_[member.size()] = '\0';

        node = (ZNode*)mem;
        node->member_ = s;
        node->socre_ = score;
        this->dict_.emplace(s, node);
    }

    this->tree_.insert(node);

    return 1;
}

std::vector<kvstr*> ZSetObject::zset_revrange(int start, int end) {
    std::vector<kvstr*> result;
    int size = this->tree_.size();

    if(start < 0) start = size + start;
    if(end < 0) end = size + end;

    if(start < 0) start = 0;
    if(start > end || start > size) return result;
    if(end >= size) end = size - 1;

    auto it = this->tree_.rbegin();
    std::advance(it, start);

    int count = end - start + 1;
    while(count > 0 && it != this->tree_.rend()) {
        result.push_back((*it)->member_);
        --count;
        ++it;
    }

    return result;
}

std::vector<kvstr*> ZSetObject::zset_range(int start, int end) {
    std::vector<kvstr*> result;
    int size = this->tree_.size();

    if(start < 0) start = size + start;
    if(end < 0) end = size + end;

    if(start < 0) start = 0;
    if(start > end || start > size) return result;
    if(end >= size) end = size - 1;

    auto it = this->tree_.begin();
    std::advance(it, start);

    int count = end - start + 1;
    while(count > 0 && it != this->tree_.end()) {
        result.push_back((*it)->member_);
        --count;
        ++it;
    }

    return result;
}

std::optional<double> ZSetObject::zset_score(std::string_view member) {
    auto it = this->dict_.getUnderlyingMap().find(member);
    if(it != this->dict_.getUnderlyingMap().end()) return it->second->socre_;

    return std::nullopt;
}

int ZSetObject::zset_card() {
    return this->tree_.size();
}

int ZSetObject::zset_rank(std::string_view member) {
    auto it = this->dict_.getUnderlyingMap().find(member);
    if(it == this->dict_.getUnderlyingMap().end()) return -1;

    ZNode* target = it->second;

    int rank{1};
    for(auto it2 = tree_.begin(); it2 != tree_.end(); ++it2) {
        if(*it2 == target) return rank;
        ++rank;
    }

    return -1;
}

int ZSetObject::zset_revrank(std::string_view member) {
    auto it = this->dict_.getUnderlyingMap().find(member);
    if(it == this->dict_.getUnderlyingMap().end()) return -1;

    ZNode* target = it->second;

    int rank{1};
    for(auto it2 = tree_.rbegin(); it2 != tree_.rend(); ++it2) {
        if(*it2 == target) return rank;
        ++rank;
    }

    return -1;
}

};