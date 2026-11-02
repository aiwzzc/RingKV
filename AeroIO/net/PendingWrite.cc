#include "PendingWrite.h"
#include "TcpConnection.h"

#include "jemalloc.h"

#include <string.h>
#include <assert.h>


namespace AeroIO {

namespace net {

// ========== ReplyBuffer ==========

ReplyBuffer::ReplyBuffer(base::JemallocWrapper* mempool) :
head_{IoType::SEND}, in_used_(false), iov_cnt_(0), total_bytes_(0),
iov_start_idx_(0), mempool_(mempool) {
    // this->string_holders_.reserve(MAX_IOV_COUNT);
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

    if(len < 128 && this->inline_offset_ + len <= META_BUF_SIZE) {
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

    auto conn = this->conn_;
    if(conn && this->head_.type_ == IoType::SEND) {
        conn->send_schedul(res_bytes);
    }
}

ReplyBufferPool::ReplyBufferPool(base::JemallocWrapper* mempool, std::size_t pool_size) :
mempool_(mempool), pool_size_(pool_size) { INIT_LIST_HEAD(&free_list_); initPool(); }

ReplyBufferPool::~ReplyBufferPool() {
    while(this->free_list_.next_ != &this->free_list_) {
        ReplyBuffer* reply = container_of(this->free_list_.next_, ReplyBuffer, node_);
        list_del(this->free_list_.next_);

        reply->~ReplyBuffer();
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

};

};
