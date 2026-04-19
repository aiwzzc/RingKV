#include "Buffer.h"
#include <string.h>
#include <iostream>
#include <algorithm>
#include "src/jemalloc.h"

namespace AeroIO {

namespace net {

char* UringBuffer::beginWrite()
{ return this->buffer_.data() + this->writerIndex_; }

std::size_t UringBuffer::writableBytes()
{ return this->buffer_.size() - this->writerIndex_; }

std::size_t UringBuffer::readableBytes()
{ return this->writerIndex_ - this->readerIndex_; }

std::size_t UringBuffer::freeBeforeSize()
{ return this->readerIndex_; }

std::size_t UringBuffer::readerIndex() const
{ return this->readerIndex_; }

void UringBuffer::hasWritten(std::size_t len)
{ this->writerIndex_ += len; }

const char* UringBuffer::peek() const
{ return this->buffer_.data() + this->readerIndex_; }

std::vector<char>&& UringBuffer::takeBuffer() {
    retrieveAll();

    return std::move(this->buffer_);
}

void UringBuffer::retrieve(std::size_t len) {
    if(len < readableBytes()) {
        this->readerIndex_ += len;

    } else {
        retrieveAll();
    }
}

void UringBuffer::retrieveAll() {
    this->readerIndex_ = 0;
    this->writerIndex_ = 0;
}

char* UringBuffer::GetBasePointer()
{ return this->buffer_.data(); }

void UringBuffer::Normalize() {
    if(this->readerIndex_ > 0) {
        ::memmove(GetBasePointer(), peek(), readableBytes());
        this->writerIndex_ -= this->readerIndex_;
        this->readerIndex_ = 0;
    }
}

void UringBuffer::EnsureFreeSpace(std::size_t len) {
    if(writableBytes() >= len) return;

    if(writableBytes() + freeBeforeSize() >= len) {
        Normalize();

    } else {
        Normalize();

        this->buffer_.resize(this->buffer_.size() + std::max(len, this->buffer_.size() / 2));
    }
}

void UringBuffer::append(char* data, std::size_t len) {
    EnsureFreeSpace(len);
    ::memcpy(beginWrite(), data, len);
    hasWritten(len);
}

const char* UringBuffer::findCRLF() {
    return this->find("\r\n", 2);
}

const char* UringBuffer::find(const char* token, std::size_t token_len) {
    if (token == nullptr || token_len == 0) return nullptr;

    const char* text = this->peek();
    std::size_t text_len = this->readableBytes();

    if(text_len < token_len) return nullptr;

    std::vector<int> next(token_len, 0);

    for (std::size_t i = 1, j = 0; i < token_len; ++i) {
        while (j > 0 && token[i] != token[j]) {
            j = next[j - 1];
        }
        if (token[i] == token[j]) {
            ++j;
        }
        next[i] = j;
    }

    for (std::size_t i = 0, j = 0; i < text_len; ++i) {
        while (j > 0 && text[i] != token[j]) {
            j = next[j - 1];
        }
        if (text[i] == token[j]) {
            ++j;
        }
        if (j == token_len) {
            return text + i - token_len + 1;
        }
    }

    return nullptr;
}

ReplBacklog::ReplBacklog(uint64_t capacity) : capacity_(capacity) {
    this->buffer_.resize(capacity);
}

uint64_t ReplBacklog::getMasterOffset() const
{ return this->master_offset_; }

uint64_t ReplBacklog::getCapacity() const
{ return this->capacity_; }

SendChunk ReplBacklog::getChunkToSend(uint64_t replica_offset) {
    if(replica_offset == this->master_offset_) return {nullptr, 0};

    std::size_t unacked = this->master_offset_ - replica_offset;
    if(unacked > this->capacity_) {
        return {nullptr, static_cast<std::size_t>(-1)};
    }

    std::size_t index = replica_offset % this->capacity_;

    std::size_t send_len = std::min(this->capacity_ - index, unacked);
    return {this->buffer_.data() + index, send_len};
}

void ReplBacklog::append(const char* data, uint64_t len) {
    if(data == nullptr || len < 0) return;

    std::size_t index = this->master_offset_ % this->capacity_;

    uint64_t first_chunk = std::min(len, this->capacity_ - index);
    ::memcpy(this->buffer_.data() + index, data, first_chunk);

    if(first_chunk < len) {
        ::memcpy(this->buffer_.data(), data + first_chunk, len - first_chunk);
    }

    this->master_offset_ += len;
}

void BufferBlock::reset() {
    this->write_index_ = 0;
    this->read_index_ = 0;
}

std::size_t BufferBlock::readableBytes()
{ return this->write_index_ - this->read_index_; }

const char* BufferBlock::peek() const
{ return this->buffer; }

void BufferBlock::hasWritten(std::size_t len) 
{ this->write_index_ += len; }

std::size_t BufferBlock::writableBytes()
{ return BUFFERBLOCKSIZE - this->write_index_; }

char* BufferBlock::beginWrite()
{ return this->buffer + this->write_index_; }

const char* BufferBlock::findCRLF()
{ return this->find("\r\n", 2); }

const char* BufferBlock::find(const char* token, std::size_t token_len) {
    if (token == nullptr || token_len == 0) return nullptr;

    const char* text = this->peek();
    std::size_t text_len = this->readableBytes();

    if(text_len < token_len) return nullptr;

    std::vector<int> next(token_len, 0);

    for (std::size_t i = 1, j = 0; i < token_len; ++i) {
        while (j > 0 && token[i] != token[j]) {
            j = next[j - 1];
        }
        if (token[i] == token[j]) {
            ++j;
        }
        next[i] = j;
    }

    for (std::size_t i = 0, j = 0; i < text_len; ++i) {
        while (j > 0 && text[i] != token[j]) {
            j = next[j - 1];
        }
        if (text[i] == token[j]) {
            ++j;
        }
        if (j == token_len) {
            return text + i - token_len + 1;
        }
    }

    return nullptr;
}

void BufferBlock::retrieve(std::size_t len) {
    if(len < this->readableBytes()) {
        this->read_index_ += len;

    } else {
        this->retrieveAll();
    }
}

void BufferBlock::retrieveAll() {
    this->read_index_ = 0;
    this->write_index_ = 0;
}

BlockPool::BlockPool(rkv::JemallocWrapper* mempool) : mempool_(mempool) {}

BlockPool::~BlockPool() {
    BufferBlock* block;
    while(pool_.try_dequeue(block)) {
        this->mempool_->free(block);
    }
}

BlockPtr BlockPool::get() {
    BufferBlock* block = nullptr;
    if(!this->pool_.try_dequeue(block)) {
        void* p = this->mempool_->alloc(sizeof(BufferBlock));
        if(p == nullptr) return nullptr;
        block = new (p) BufferBlock();
    }

    block->reset();

    return std::shared_ptr<BufferBlock>(block, [this] (BufferBlock* block) {
        this->release(block);
    });
}

void BlockPool::release(BufferBlock* block) {
    this->pool_.enqueue(block);
}

};

};