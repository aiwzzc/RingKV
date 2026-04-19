#pragma once
#include <cstddef>

namespace AeroIO {
namespace net {
    class ReplyBufferPool;
    class BlockPool;
};
};

namespace rkv {

class JemallocWrapper;
class Ringengine;

struct ServerContext {
    rkv::JemallocWrapper* mempool;
    rkv::Ringengine* engine;
    AeroIO::net::ReplyBufferPool* replyBufPool;
    AeroIO::net::BlockPool* blockPool;
};

constexpr int KVS_OK = 0;
constexpr int KVS_ERR_NULL = -1;       // 内存分配失败
constexpr int KVS_ERR_TYPE = -2;       // 类型错误
constexpr int KVS_ERR_NOT_INT = -3;    // 无法解析为整数
constexpr int KVS_ERR_OVERFLOW = -4;   // 数值溢出/下溢

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

void incrRefCount(RedisObject* obj);
void decrRefCount(RedisObject* obj, JemallocWrapper* mempool);
bool string2ll(const char* data, size_t len, long long* val);

};