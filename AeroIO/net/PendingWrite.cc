#include "PendingWrite.h"
#include "TcpConnection.h"

#include <charconv>
#include <string.h>
#include "src/common.h"
#include "src/jemalloc.h"
#include "src/CommandHandlers.h"
#include "src/kvstr.h"
#include "EventLoop.h"
#include <iostream>
#include <assert.h>


namespace AeroIO {

namespace net {

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

// ========== PendingWrite ==========

PendingWrite::PendingWrite(rkv::JemallocWrapper* mempool):
mempool_(mempool) {}

PendingWrite::~PendingWrite()
{ reset(); }

void PendingWrite::setType(IoType type)
{ this->head_.type_ = type; }

void PendingWrite::append(const char* data, std::size_t len) {
    if(this->iov_cnt_ >= MAX_IOV_COUNT) return;
    void* p = nullptr;

    if(this->inline_offset_ + len <= META_BUF_SIZE) {
        p = this->inline_buf_ + this->inline_offset_;
        this->inline_offset_ += len;

    } else {
        p = this->mempool_->alloc(len);
        if(p == nullptr) return;

        this->dynamic_allocated_ptrs_.push_back(p);
    }

    ::memcpy(p, data, len);

    this->iovs_[this->iov_cnt_].iov_base = (void*)p;
    this->iovs_[this->iov_cnt_].iov_len = len;

    ++this->iov_cnt_;
    this->total_bytes_ += len;
}

void PendingWrite::reset() {
    this->iov_cnt_ = 0;
    this->total_bytes_ = 0;
    this->iov_start_idx_ = 0;

    for(auto* p : this->dynamic_allocated_ptrs_) {
        this->mempool_->free(p);
    }

    this->dynamic_allocated_ptrs_.clear();
}

void PendingWrite::adjustForPartialWrite(std::size_t write_bytes) {
    this->total_bytes_ -= write_bytes;

    while(write_bytes >= 0 && this->iov_start_idx_ < this->iov_cnt_) {
        if(write_bytes >= this->iovs_[this->iov_start_idx_].iov_len) {
            write_bytes -= this->iovs_[this->iov_start_idx_].iov_len;
            ++this->iov_start_idx_;

        } else {
            this->iovs_[this->iov_start_idx_].iov_base = (char*)this->iovs_[this->iov_start_idx_].iov_base + write_bytes;
            this->iovs_[this->iov_start_idx_].iov_len -= write_bytes;

            write_bytes = 0;
        }
    }
}

void PendingWrite::onComplete(int res_bytes) {

    if(this->conn_ && this->head_.type_ == IoType::HTTP_SEND) {
        if(res_bytes >= 0) {
            if(res_bytes == this->total_bytes_) {
                this->reset();

            } else if(res_bytes < this->total_bytes_) {
                this->adjustForPartialWrite(res_bytes);
                this->conn_->send();
            }
        }
    }
}

PendingWritePool::PendingWritePool(rkv::JemallocWrapper* mempool, std::size_t pool_size):
mempool_(mempool), pool_size_(pool_size) { INIT_LIST_HEAD(&this->free_list_); initPool(); }

PendingWritePool::~PendingWritePool() {
    while(this->free_list_.next_ != &this->free_list_) {
        PendingWrite* pw = container_of(this->free_list_.next_, PendingWrite, node_);
        list_del(this->free_list_.next_);

        this->mempool_->free(pw);
    }
}

PendingWritePtr PendingWritePool::get() {
    PendingWrite* pw = nullptr;
    if(this->free_list_.next_ == &this->free_list_) {
        void* p = this->mempool_->alloc(sizeof(PendingWrite));
        pw = new (p) PendingWrite(this->mempool_);
        pw->setType(IoType::HTTP_SEND);

        return std::shared_ptr<PendingWrite>(pw, [this] (PendingWrite* pw) {
            this->release(pw);
        });
    }

    list_head* node = this->free_list_.next_;
    list_del(node);

    pw = container_of(node, PendingWrite, node_);
    pw->setType(IoType::HTTP_SEND);

    return std::shared_ptr<PendingWrite>(pw, [this] (PendingWrite* pw) {
        this->release(pw);
    });
}

void PendingWritePool::release(PendingWrite* pw) {
    if(pw == nullptr) return;

    reset(pw);
    list_add(&this->free_list_, &pw->node_);
}

void PendingWritePool::initPool() {
    for(int i = 0; i < this->pool_size_; ++i) {
        void* p = this->mempool_->alloc(sizeof(PendingWrite));
        PendingWrite* pw = new (p) PendingWrite(this->mempool_);
        pw->setType(IoType::HTTP_SEND);

        list_add(&this->free_list_, &pw->node_);
    }
}

void PendingWritePool::reset(PendingWrite* pw) {
    pw->in_used_ = false;
    pw->iov_cnt_ = 0;
    pw->total_bytes_ = 0;
    pw->iov_start_idx_ = 0;

    for(auto* p : pw->dynamic_allocated_ptrs_) {
        this->mempool_->free(p);
    }

    pw->dynamic_allocated_ptrs_.clear();

    pw->conn_.reset();
}

// ========== ReplyBuffer ==========

ReplyBuffer::ReplyBuffer(rkv::JemallocWrapper* mempool) : 
mempool_(mempool) {
    this->string_holders_.reserve(MAX_IOV_COUNT);
}

void ReplyBuffer::setType(IoType type)
{ this->head_.type_ = type; }

void ReplyBuffer::setConn(TcpConnectionPtr conn)
{ this->conn_ = conn; }

bool ReplyBuffer::isNearlyFull() const {
    return (iov_cnt_ >= MAX_IOV_COUNT - 4);
}

void ReplyBuffer::appendString(std::string&& data) {
    assert(this->iov_cnt_ >= 0 && this->iov_cnt_ < MAX_IOV_COUNT);
    if(this->iov_cnt_ >= MAX_IOV_COUNT) return;

    std::size_t len = data.size();
    if(len == 0) return;

    if(len < 128 && this->inline_offset_ + len <= MAX_IOV_COUNT) {
        char* start = this->inline_buf_ + this->inline_offset_;
        ::memcpy(start, data.data(), len);

        if(this->iov_cnt_ > 0 && 
            this->iovs_[this->iov_cnt_ - 1].iov_base == start - this->iovs_[this->iov_cnt_ - 1].iov_len) {
            this->iovs_[this->iov_cnt_ - 1].iov_len += len;

        } else {
            this->iovs_[this->iov_cnt_].iov_base = start;
            this->iovs_[this->iov_cnt_].iov_len = len;
            ++this->iov_cnt_;
        }

        this->total_bytes_ += len;
        this->inline_offset_ += len;

    } else {
        this->string_holders_.push_back(std::move(data));

        const auto& holding_str = this->string_holders_.back();

        this->iovs_[this->iov_cnt_].iov_base = (void*)holding_str.data();
        this->iovs_[this->iov_cnt_].iov_len = holding_str.size();

        ++this->iov_cnt_;
        this->total_bytes_ += holding_str.size();
    }
}

void ReplyBuffer::appendStatic(const char* str, std::size_t len) {
    assert(this->iov_cnt_ >= 0 && this->iov_cnt_ < MAX_IOV_COUNT);
    if(this->iov_cnt_ >= MAX_IOV_COUNT) return;

    void* p = nullptr;

    if(this->inline_offset_ + len <= META_BUF_SIZE) {
        p = this->inline_buf_ + this->inline_offset_;
        this->inline_offset_ += len;

    } else {
        p = this->mempool_->alloc(len);
        if(p == nullptr) return;

        this->dynamic_allocated_ptrs_.push_back(p);
    }

    if(p == nullptr) return;
    ::memcpy(p, str, len);

    this->iovs_[this->iov_cnt_].iov_base = p;
    this->iovs_[this->iov_cnt_].iov_len = len;

    ++this->iov_cnt_;
    this->total_bytes_ += len;
}

void ReplyBuffer::adjustForPartialWrite(int written_bytes) {
    this->total_bytes_ -= written_bytes;

    while(written_bytes > 0 && this->iov_start_idx_ < this->iov_cnt_) {

        std::size_t current_iovec_size = this->iovs_[this->iov_start_idx_].iov_len;
        if(written_bytes >= current_iovec_size) {
            written_bytes -= current_iovec_size;
            ++this->iov_start_idx_;

        } else {
            this->iovs_[this->iov_start_idx_].iov_base = (char*)this->iovs_[this->iov_start_idx_].iov_base + written_bytes;
            this->iovs_[this->iov_start_idx_].iov_len -= written_bytes;

            written_bytes = 0;
        }

    }
}

void ReplyBuffer::onComplete(int res_bytes) {

    if(this->conn_ && this->head_.type_ == IoType::SEND) {
        this->conn_->send_schedul(res_bytes);
    }
}

ReplyBufferPool::ReplyBufferPool(rkv::JemallocWrapper* mempool, std::size_t pool_size) :
mempool_(mempool), pool_size_(pool_size) { INIT_LIST_HEAD(&free_list_); initPool(); }

ReplyBufferPool::~ReplyBufferPool() {
    while(this->free_list_.next_ != &this->free_list_) {
        ReplyBuffer* reply = container_of(this->free_list_.next_, ReplyBuffer, node_);
        list_del(this->free_list_.next_);

        this->mempool_->free(reply);
    }
}

ReplyBufferPtr ReplyBufferPool::get() {
    ReplyBuffer* reply = nullptr;
    if(this->free_list_.next_ == &this->free_list_) {
        void* ptr = this->mempool_->alloc(sizeof(ReplyBuffer));
        reply = new (ptr) ReplyBuffer(this->mempool_);
        reply->setType(IoType::SEND);
        
        return std::shared_ptr<ReplyBuffer>(reply, [this] (ReplyBuffer* r) {
            this->release(r);
        });
    }

    list_head* node = this->free_list_.next_;
    list_del(node);

    reply = container_of(node, ReplyBuffer, node_);
    reply->setType(IoType::SEND);

    return std::shared_ptr<ReplyBuffer>(reply, [this] (ReplyBuffer* reply) {
        this->release(reply);
    });
}

void ReplyBufferPool::release(ReplyBuffer* reply) {
    if(reply == nullptr) return;

    reset(reply);
    list_add(&this->free_list_, &reply->node_);
}

void ReplyBufferPool::initPool() {
    for(int i = 0; i < this->pool_size_; ++i) {
        void* ptr = this->mempool_->alloc(sizeof(ReplyBuffer));
        ReplyBuffer* reply = new (ptr) ReplyBuffer(this->mempool_);
        reply->setType(IoType::SEND);

        list_add(&this->free_list_, &reply->node_);
    }
}

void ReplyBufferPool::reset(ReplyBuffer* reply) {
    if(reply == nullptr) return;

    reply->in_used_ = false;
    reply->iov_cnt_ = 0;
    reply->total_bytes_ = 0;
    reply->iov_start_idx_ = 0;

    reply->inline_offset_ = 0;

    for(void* p : reply->dynamic_allocated_ptrs_) {
        this->mempool_->free(p);
    }
    reply->dynamic_allocated_ptrs_.clear();

    reply->string_holders_.clear();
    
    reply->conn_.reset();
}

void RouteBatchTask::reset() {
    for(int i = 0; i < this->cmd_count_; ++i) {
        this->cmds[i].buffer_ = std::string("");
        this->cmds[i].tokens_.clear();
    }

    this->cmd_count_ = 0;
    this->conn_.reset();
}

void RouteBatchTask::operator()() {

    std::unordered_map<std::string_view, std::string> local_notwrite_cache;
    std::unordered_map<std::string_view, std::pair<std::vector<std::string_view>, std::vector<int>>> local_last_write_slot;

    for(int i = 0; i < this->cmd_count_; ++i) {
        if(local_notwrite_cache.contains(this->cmds[i].tokens_[1]) && !this->cmds[i].cmd_def_->is_write_) {
            std::string hot_data = local_notwrite_cache[this->cmds[i].tokens_[1]];
            if(cmds[i].slot_id_ != 0) this->cmds[i].response_data_ = std::move(hot_data);
            continue;

        } else if(this->cmds[i].cmd_def_->is_write_) {
            local_last_write_slot[this->cmds[i].tokens_[1]].first = this->cmds[i].tokens_;
            local_last_write_slot[this->cmds[i].tokens_[1]].second.push_back(i);
            continue;
        }

        rkv::CommandContext ctx{cmds[i].tokens_, this->target_engine_, true, this->current_loop_, this->target_loop_, "", false};

        this->execute_(ctx, cmds[i].cmd_def_);
        if(cmds[i].slot_id_ != 0) {
            if(!this->cmds[i].cmd_def_->is_write_) local_notwrite_cache[this->cmds[i].tokens_[1]] = ctx.response_;
            cmds[i].response_data_ = std::move(ctx.response_);
        }
    }

    for(auto it = local_last_write_slot.begin(); it != local_last_write_slot.end(); ++it) {
        rkv::CommandContext ctx{it->second.first, this->target_engine_, true, this->current_loop_, this->target_loop_, "", false};
        this->execute_(ctx, this->cmds[it->second.second.back()].cmd_def_);

        for(auto& task_index : it->second.second) {
            std::string delay_res = ctx.response_;

            if(this->cmds[task_index].slot_id_ != 0) {
                this->cmds[task_index].response_data_ = std::move(delay_res);
            }
        }
    }

    this->current_loop_->runInLoop([this] () mutable {
        if(this->conn_) {
            for(int i = 0; i < this->cmd_count_; ++i) {
                if(this->cmds[i].slot_id_ != 0) {
                    this->conn_->fillSingleSlot(this->cmds[i].slot_id_, std::move(this->cmds[i].response_data_));
                }
            }

            this->conn_->tryFlushResponses();
        }
    });
}

RouteBatchTaskPool::RouteBatchTaskPool() { INIT_LIST_HEAD(&this->head_); }
RouteBatchTaskPool::~RouteBatchTaskPool() {
    while(this->head_.next_ != &this->head_) {
        RouteBatchTask* task = container_of(this->head_.next_, RouteBatchTask, node_);
        list_del(this->head_.next_);

        this->mempool_->free(task);
    }
}

void RouteBatchTaskPool::initPool() {
    for(int i = 0; i < TASKPOOLSIZE; ++i) {
        void*p = this->mempool_->alloc(sizeof(RouteBatchTask));
        if(p == nullptr) return;

        RouteBatchTask* task = new (p) RouteBatchTask();
        list_add(&this->head_, &task->node_);
    }
}

RouteBatchTaskPool& RouteBatchTaskPool::getInstance() {
    static thread_local RouteBatchTaskPool instance;
    return instance;
}

RouteBatchTaskPtr RouteBatchTaskPool::get(EventLoop* current_loop) {
    RouteBatchTask* task = nullptr;
    if(this->head_.next_ == &this->head_) {
        void*p = this->mempool_->alloc(sizeof(RouteBatchTask));
        if(p == nullptr) return nullptr;

        task = new (p) RouteBatchTask();

        return std::shared_ptr<RouteBatchTask>(task, [current_loop] (RouteBatchTask* Task) {
            current_loop->runInLoop([Task] () {
                RouteBatchTaskPool::getInstance().release(Task);
            });
        });
    }

    task = container_of(this->head_.next_, RouteBatchTask, node_);
    list_del(this->head_.next_);

    return std::shared_ptr<RouteBatchTask>(task, [current_loop] (RouteBatchTask* Task) {
        current_loop->runInLoop([Task] () {
            RouteBatchTaskPool::getInstance().release(Task);
        });
    });
}

void RouteBatchTaskPool::release(RouteBatchTask* task) {
    task->reset();
    list_add(&this->head_, &task->node_);
}

void RouteBatchTaskPool::setMempool(rkv::JemallocWrapper* mempool)
{ this->mempool_ = mempool; }

};

};