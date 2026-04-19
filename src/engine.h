#pragma once

#include <span>
#include <string>
#include <functional>
#include <memory>

#include "common.h"
#include "dict.h"

namespace rkv {

class JemallocWrapper;
struct kvstr;
using EngineMap = ankerl::unordered_dense::map<kvstr*, RedisObject*, KvstrHash, KvstrEqual>;

class Ringengine {
protected:
    JemallocWrapper* mempool_;

public:
    friend class KvsProtocolHandler;

    Ringengine(JemallocWrapper* pool) : mempool_(pool) {}
    virtual ~Ringengine() = default;

    virtual JemallocWrapper* mempool() const = 0;

    virtual EngineMap& getUnderlyingMap() = 0;
    

    // Global Command
    virtual int del(std::string_view) = 0;
    virtual void flushall() = 0;
    virtual RedisObject* getMeta(std::string_view) = 0;

    // String Command
    virtual bool exist(std::string_view) = 0;
    virtual int set(std::string_view, std::string_view) = 0;
    virtual int setnx(std::string_view, std::string_view) = 0;
    virtual RedisObject* get(std::string_view) = 0;
    virtual int incr(std::string_view, long long*) = 0;
    virtual int incrby(std::string_view, int, long long*) = 0;
    virtual int decr(std::string_view, long long*) = 0;
    virtual int decrby(std::string_view, int, long long*) = 0;

    // List Command
    virtual int lpush(std::string_view, std::vector<std::string_view>&) = 0;
    virtual int rpush(std::string_view, std::vector<std::string_view>&) = 0;
    virtual int lset(std::string_view, int, std::string_view) = 0;
    virtual std::vector<kvstr*> lpop(std::string_view, std::size_t) = 0;
    virtual std::vector<kvstr*> rpop(std::string_view, std::size_t) = 0;
    virtual kvstr* lindex(std::string_view, int) = 0;
    virtual std::size_t llen(std::string_view) = 0;
    virtual std::vector<kvstr*> lrange(std::string_view, int, int) = 0;

    // Hash Command
    virtual int hset(std::string_view, std::vector<std::string_view>&) = 0;
    virtual bool hget(std::string_view, std::string_view) = 0;
    virtual int hdel(std::string_view, std::vector<std::string_view>&) = 0;
    virtual bool hexists(std::string_view, std::string&) = 0;
    virtual std::size_t hlen(std::string_view) = 0;
    virtual std::vector<kvstr*> hgetall(std::string_view) = 0;

    // Set Command
    // virtual int sadd(std::string_view, std::string_view) = 0;

    // ZSet Command
    virtual int zadd(std::string_view, std::vector<std::string_view>&) = 0;
    virtual std::vector<kvstr*> zrevrange(std::string_view, int, int) = 0;
    virtual std::vector<kvstr*> zrange(std::string_view, int, int) = 0;
    virtual std::optional<double> zscore(std::string_view, std::string_view) = 0;
    virtual int zcard(std::string_view) = 0;
    virtual std::optional<int> zrank(std::string_view, std::string_view) = 0;
    virtual std::optional<int> zrevrank(std::string_view, std::string_view) = 0;

    // static std::unique_ptr<kvs_engine> create(const Config*, Mem_Pool*);
};

class rdict : public Ringengine {

private:
    rhash<RedisObject*> dict_;

public:
    explicit rdict(JemallocWrapper* pool);
    ~rdict() = default;

    EngineMap& getUnderlyingMap() override;

    JemallocWrapper* mempool() const;

    // Global Command
    int del(std::string_view) override;
    void flushall() override;
    RedisObject* getMeta(std::string_view) override;

    // String Command
    bool exist(std::string_view) override;
    int set(std::string_view, std::string_view) override;
    int setnx(std::string_view, std::string_view) override;
    RedisObject* get(std::string_view) override;
    int incr(std::string_view, long long*) override;
    int incrby(std::string_view, int, long long*) override;
    int decr(std::string_view, long long*) override;
    int decrby(std::string_view, int, long long*) override;

    // List Command
    int lpush(std::string_view, std::vector<std::string_view>&) override;
    int rpush(std::string_view, std::vector<std::string_view>&) override;
    int lset(std::string_view, int, std::string_view) override;
    std::vector<kvstr*> lpop(std::string_view, std::size_t) override;
    std::vector<kvstr*> rpop(std::string_view, std::size_t) override;
    kvstr* lindex(std::string_view, int) override;
    std::size_t llen(std::string_view) override;
    std::vector<kvstr*> lrange(std::string_view, int, int) override;

    // Hash Command
    int hset(std::string_view, std::vector<std::string_view>&) override;
    bool hget(std::string_view, std::string_view) override;
    int hdel(std::string_view, std::vector<std::string_view>&) override;
    bool hexists(std::string_view, std::string&) override;
    std::size_t hlen(std::string_view) override;
    std::vector<kvstr*> hgetall(std::string_view) override;

    // ZSet Command
    int zadd(std::string_view, std::vector<std::string_view>&) override;
    std::vector<kvstr*> zrevrange(std::string_view, int, int) override;
    std::vector<kvstr*> zrange(std::string_view, int, int) override;
    std::optional<double> zscore(std::string_view, std::string_view) override;
    int zcard(std::string_view) override;
    std::optional<int> zrank(std::string_view, std::string_view) override;
    std::optional<int> zrevrank(std::string_view, std::string_view) override;
};


};