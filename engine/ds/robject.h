#pragma once

#include <cstddef>

namespace rkv {

class JemallocWrapper;

enum RedisType {
    OBJ_STRING = 0,
    OBJ_LIST,
    OBJ_SET,
    OBJ_ZSET,
    OBJ_HASH
};

enum RedisStringEncodingType {
    OBJ_ENCODING_RAW = 0,
    OBJ_ENCODING_INT,
    OBJ_ENCODING_EMBSTR
};

struct RedisObject {
    unsigned type : 4;
    unsigned encoding : 4;
    std::size_t len;
    std::size_t refcount;
    void* ptr;
};

class robject {

public:
    static void freeRedisObject(RedisObject* obj, JemallocWrapper* mempool);

    static void incrRefCount(RedisObject* obj);
    static void decrRefCount(RedisObject* obj, JemallocWrapper* mempool);
    static bool string2ll(const char* data, size_t len, long long* val);

private:


};

};