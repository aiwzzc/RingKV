#include "dict.h"
#include "common.h"
#include "jemalloc.h"
#include "kvstr.h"
#include "wyhash/wyhash.h"

namespace {

static inline uint64_t fast_hash(const void* data, size_t len) {
    return wyhash(data, len, 0, _wyp);
}

};

namespace rkv {

inline std::string_view kvstr_to_sv(const kvstr* s)
{ return std::string_view(s->buf_, s->len_); }


std::size_t KvstrHash::operator()(const kvstr* s) const {
    auto sv = kvstr_to_sv(s);
    return fast_hash(sv.data(), sv.size());
}

std::size_t KvstrHash::operator()(std::string_view sv) const {
    return fast_hash(sv.data(), sv.size());
}

bool KvstrEqual::operator()(const kvstr* a, const kvstr* b) const {
    return kvstr_to_sv(a) == kvstr_to_sv(b);
}

bool KvstrEqual::operator()(const kvstr* a, std::string_view b) const {
    return kvstr_to_sv(a) == b;
}

bool KvstrEqual::operator()(std::string_view a, const kvstr* b) const {
    return a == kvstr_to_sv(b);
}

RedisObject* HashObject::CreateHashObject(JemallocWrapper* mempool) {
    if(mempool == nullptr) return nullptr;

    RedisObject* obj = (RedisObject*)mempool->alloc(sizeof(RedisObject));
    if(obj == nullptr) return nullptr;

    obj->type = OBJ_HASH;
    obj->len = 0;
    obj->refcount = 1;

    void* p = mempool->alloc(sizeof(HashObject));
    if(p == nullptr) return nullptr;

    HashObject* hobj = new (p) HashObject();
    obj->ptr = (void*)hobj;

    return obj;
}

int HashObject::hash_set(std::string_view field, std::string_view value) {
    
}

std::optional<kvstr*> HashObject::hash_get(std::string_view field) {

}

int HashObject::hash_del(std::string_view field) {

}

std::size_t HashObject::hash_len() {

}

bool HashObject::hash_exists(std::string_view field) {

}

std::vector<std::pair<kvstr*, kvstr*>> HashObject::hash_get_all() {

}

};