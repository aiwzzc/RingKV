#include "common.h"
#include "jemalloc.h"
#include "robject.h"
#include <chrono>

namespace rkv {

void incrRefCount(RedisObject* obj) {
    if(obj == nullptr) return;

    ++obj->refcount;
}

void decrRefCount(RedisObject* obj, JemallocWrapper* mempool) {
    if(obj == nullptr || mempool == nullptr || obj->refcount <= 0) return;

    if(obj->refcount > 0) {
        --obj->refcount;

        if(obj->refcount == 0) { robject::freeRedisObject(obj, mempool); }
    }
}

bool string2ll(const char* data, size_t len, long long* val) {
    if(data == nullptr || val == nullptr) return false;

    char* strend;
    long long llv = std::strtoll(data, &strend, 10);

    if((strend - data) == len && len > 0) {
        *val = llv;

        return true;
    }

    return false;
}

};