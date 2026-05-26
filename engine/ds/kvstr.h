#pragma once

namespace rkv {

constexpr int EMBSTR_MAX_LEN = 44;

class JemallocWrapper;

struct RedisObject;

struct kvstr {
    int len_;
    int free_;
    char buf_[];

    static kvstr* KvstrCreate(const char* initdata, int initlen, JemallocWrapper* mempool);
    static RedisObject* CreateStringObject(const char* data, int datalen, JemallocWrapper* mempool);
    static RedisObject* CreateRawStringObject(const char* data, int datalen, JemallocWrapper* mempool);
    static RedisObject* CreateIntStringObject(long long val, JemallocWrapper* mempool);
};

};