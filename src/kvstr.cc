#include "kvstr.h"
#include "common.h"
#include "jemalloc.h"
#include <string.h>

namespace rkv {

kvstr* kvstr::KvstrCreate(const char* initdata, int initlen, JemallocWrapper* mempool) {
    if(mempool == nullptr) return nullptr;
    if(initlen < 0) return nullptr;
    if(initdata == nullptr && initlen > 0) return nullptr;

    size_t total_len = sizeof(kvstr) + initlen + 1;
    void* mem = mempool->alloc(total_len);
    if(mem == nullptr) return nullptr;

    kvstr* s = (kvstr*)mem;
    s->len_ = initlen;
    s->free_ = 0;

    if(initdata && initlen > 0) ::memcpy(s->buf_, initdata, initlen);
    s->buf_[initlen] = '\0';

    return s;
}

RedisObject* kvstr::CreateStringObject(const char* data, int datalen, JemallocWrapper* mempool) {
    if(data == nullptr || mempool == nullptr || datalen < 0) return nullptr;

    if(datalen <= EMBSTR_MAX_LEN) {
        std::size_t total_len = sizeof(RedisObject) + sizeof(kvstr) + datalen + 1;
        void* ptr = mempool->alloc(total_len);
        if(ptr == nullptr) return nullptr;

        kvstr* s = (kvstr*)((char*)ptr + sizeof(RedisObject));
        if(datalen > 0) ::memcpy(s->buf_, data, datalen);
        s->buf_[datalen] = '\0';
        s->len_ = datalen;
        s->free_ = 0;

        RedisObject* obj = (RedisObject*)ptr;
        obj->len = datalen;
        obj->refcount = 1;
        obj->type = OBJ_STRING;
        obj->encoding = OBJ_ENCODING_EMBSTR;
        obj->ptr = (void*)s;

        return obj;
    }

    return CreateRawStringObject(data, datalen, mempool);
}

RedisObject* kvstr::CreateRawStringObject(const char* data, int datalen, JemallocWrapper* mempool) {
    if(data == nullptr || mempool == nullptr || datalen < 0) return nullptr;

    RedisObject* obj = (RedisObject*)mempool->alloc(sizeof(RedisObject));
    if(obj == nullptr) return nullptr;

    kvstr* s = kvstr::KvstrCreate(data, datalen, mempool);
    if(s == nullptr) {
        mempool->free(obj);
        return nullptr;
    }

    obj->len = datalen;
    obj->refcount = 1;
    obj->type = OBJ_STRING;
    obj->encoding = OBJ_ENCODING_RAW;
    obj->ptr = (void*)s;

    return obj;
}

RedisObject* kvstr::CreateIntStringObject(long long val, JemallocWrapper* mempool) {
    if(mempool == nullptr) return nullptr;

    RedisObject* obj = (RedisObject*)mempool->alloc(sizeof(RedisObject));
    if(obj == nullptr) return nullptr;

    obj->encoding = OBJ_ENCODING_INT;
    obj->type = OBJ_STRING;
    obj->len = 0;
    obj->refcount = 1;
    obj->ptr = (void*)val;

    return obj;
}

};