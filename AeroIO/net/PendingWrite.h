#pragma once

#include <memory>
#include <vector>
#include <atomic>
#include <sys/uio.h>
#include "IoRequest.h"
#include "common.h"

namespace rkv {

struct RedisObject;
class JemallocWrapper;
struct kvstr;
class Ringengine;
struct CommandContext;
struct CommandDef;
class KvsProtocolHandler;

};

namespace AeroIO {

namespace net {

#define container_of(ptr, type, member) \
    ((type*)((char*)(ptr) - offsetof(type, member)))

class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

constexpr std::size_t PENDINGWRITEDATASIZE = 1024;

struct list_head {
    struct list_head* prev_;
    struct list_head* next_;
};

#define MAX_IOV_COUNT 4096
#define META_BUF_SIZE 4096

struct PendingWrite {
    GeneralHead head_;
    struct list_head node_;

    TcpConnectionPtr conn_;
    bool in_used_;

    char inline_buf_[META_BUF_SIZE]; 
    std::size_t inline_offset_ = 0;

    std::vector<void*> dynamic_allocated_ptrs_;

    struct iovec iovs_[MAX_IOV_COUNT];
    int iov_cnt_;
    std::size_t total_bytes_;
    std::size_t iov_start_idx_;
    rkv::JemallocWrapper* mempool_;

    PendingWrite(rkv::JemallocWrapper* mempool);
    ~PendingWrite();

    void onComplete(int res_bytes);
    void setType(IoType type);
    void append(const char*, std::size_t);
    void reset();
    void adjustForPartialWrite(std::size_t write_bytes);
};

using PendingWritePtr = std::shared_ptr<PendingWrite>;

class PendingWritePool {

public:
    PendingWritePool(rkv::JemallocWrapper* mempool, std::size_t pool_size);
    ~PendingWritePool();

    PendingWritePtr get();
    void release(PendingWrite* pw);
    void initPool();

private:
    void reset(PendingWrite* pw);

    struct list_head free_list_;
    std::size_t pool_size_;
    rkv::JemallocWrapper* mempool_;
};

// ============ ReplyBuffer =============

struct ReplyBuffer {

    GeneralHead head_;
    struct list_head node_;

    TcpConnectionPtr conn_;
    bool in_used_;

    char inline_buf_[META_BUF_SIZE]; 
    std::size_t inline_offset_ = 0;

    std::vector<void*> dynamic_allocated_ptrs_;

    struct iovec iovs_[MAX_IOV_COUNT];
    int iov_cnt_;
    std::size_t total_bytes_;
    std::size_t iov_start_idx_;

    rkv::JemallocWrapper* mempool_;
    std::vector<std::string> string_holders_;

    ReplyBuffer(rkv::JemallocWrapper*);

    void setType(IoType type);
    void setConn(TcpConnectionPtr conn);
    void appendString(std::string&& data);
    void appendStatic(const char*, std::size_t);
    void adjustForPartialWrite(int written_bytes);
    void onComplete(int res_bytes);
    bool isNearlyFull() const;
};

using ReplyBufferPtr = std::shared_ptr<ReplyBuffer>;

class ReplyBufferPool {

public:
    ReplyBufferPool(rkv::JemallocWrapper* mempool, std::size_t pool_size);
    ~ReplyBufferPool();

    ReplyBufferPtr get();
    void release(ReplyBuffer* pw);
    void initPool();

private:
    void reset(ReplyBuffer* pw);

    struct list_head free_list_;
    std::size_t pool_size_;
    rkv::JemallocWrapper* mempool_;

};

// ============ RouteBatchTask =============

using execute_cmd = void(*)(rkv::CommandContext&, const rkv::CommandDef*);

struct RouteBatchTask {

    struct list_head node_;

    RoutedCommand cmds[64];
    int cmd_count_{0};

    TcpConnectionPtr conn_;
    rkv::Ringengine* target_engine_;
    EventLoop* target_loop_;
    EventLoop* current_loop_;
    execute_cmd execute_;

    void reset();
    void operator()();
    
};

using RouteBatchTaskPtr = std::shared_ptr<RouteBatchTask>;

constexpr std::size_t TASKPOOLSIZE = 512;

class RouteBatchTaskPool {

private:
    struct list_head head_;
    std::size_t pool_size_;
    rkv::JemallocWrapper* mempool_;

public:
    RouteBatchTaskPool();
    ~RouteBatchTaskPool();

    static RouteBatchTaskPool& getInstance();
    RouteBatchTaskPtr get(EventLoop* current_loop);
    void release(RouteBatchTask* task);
    void initPool();
    void setMempool(rkv::JemallocWrapper* mempool);

};

};

};