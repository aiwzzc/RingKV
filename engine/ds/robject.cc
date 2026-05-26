#include "robject.h"

#include "jemalloc.h"
#include "list.h"
#include "kvstr.h"
#include "zset.h"

namespace rkv {

void robject::freeRedisObject(RedisObject* obj, JemallocWrapper* mempool) {
    if(obj == nullptr) return;

    switch(obj->type) {
        case RedisType::OBJ_STRING: {
            if(obj->ptr && obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
            
                mempool->free(s);
            }
            mempool->free(obj);

            break;
        }

        case RedisType::OBJ_LIST: {
            if(obj->ptr) {
                ListObject* list = (ListObject*)obj->ptr;
                list->release_all_node(mempool);

                mempool->free(obj->ptr);
            }
            mempool->free(obj);

            break;
        }

        case RedisType::OBJ_ZSET: {
            if(obj->ptr) {
                ZSetObject* zset = (ZSetObject*)obj->ptr;
                zset->release_all_node(mempool);

                mempool->free(obj->ptr);
            }
            mempool->free(obj);

            break;
        }
    }
}

void robject::incrRefCount(RedisObject* obj) {
    if(obj == nullptr) return;

    ++obj->refcount;
}

void robject::decrRefCount(RedisObject* obj, JemallocWrapper* mempool) {
    if(obj == nullptr || mempool == nullptr || obj->refcount <= 0) return;

    if(obj->refcount > 0) {
        --obj->refcount;

        if(obj->refcount == 0) { robject::freeRedisObject(obj, mempool); }
    }
}

bool robject::string2ll(const char* data, size_t len, long long* val) {
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

