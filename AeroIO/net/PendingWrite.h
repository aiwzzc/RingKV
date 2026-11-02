#pragma once

#include "jemalloc.h"

#include <memory>
#include <vector>
#include <deque>
#include <sys/uio.h>

#include "IoRequest.h"
#include "common.h"


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

inline void INIT_LIST_HEAD(list_head* head) {
    head->next_ = head;
    head->prev_ = head;
}

inline void list_add(list_head* head, list_head* node) {
    node->next_ = head->next_;
    node->prev_ = head;
    head->next_->prev_ = node;
    head->next_ = node;
}

inline void list_del(list_head* node) {
    node->prev_->next_ = node->next_;
    node->next_->prev_ = node->prev_;

    node->next_ = nullptr;
    node->prev_ = nullptr;
}

#define MAX_IOV_COUNT 4096
#define META_BUF_SIZE 4096

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

    base::JemallocWrapper* mempool_;
    std::deque<std::string> string_holders_;

    ReplyBuffer(base::JemallocWrapper*);

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
    ReplyBufferPool(base::JemallocWrapper* mempool, std::size_t pool_size);
    ~ReplyBufferPool();

    ReplyBufferPtr get();
    void release(ReplyBuffer* pw);
    void initPool();

private:
    void reset(ReplyBuffer* pw);

    struct list_head free_list_;
    std::size_t pool_size_;
    base::JemallocWrapper* mempool_;

};

};

};