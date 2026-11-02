#pragma once

#include "jemalloc.h"

#include <cstddef>
#include <vector>
#include <memory>

#include "ConcurrentQueue.h"


namespace AeroIO {

namespace net {

class UringBuffer {

public:
    static const size_t kInitialSize            = 65536;
    static const std::size_t kMinWritableBytes  = 4096; // Low Watermark

    UringBuffer() : buffer_(kInitialSize), readerIndex_(0), writerIndex_(0) {};

    UringBuffer(const UringBuffer&) = delete;
    UringBuffer& operator=(const UringBuffer&) = delete;

    UringBuffer(UringBuffer&& other) noexcept :
    buffer_(std::move(other.buffer_)), readerIndex_(other.readerIndex_), 
    writerIndex_(other.writerIndex_) {

        other.readerIndex_ = 0;
        other.writerIndex_ = 0;
    }

    UringBuffer& operator=(UringBuffer&& other) noexcept {
        if(this != &other) {
            this->buffer_ = std::move(other.buffer_);
            this->readerIndex_ = other.readerIndex_;
            this->writerIndex_ = other.writerIndex_;

            other.readerIndex_ = 0;
            other.writerIndex_ = 0;
        }

        return *this;
    }

    char* beginWrite();
    std::size_t writableBytes();
    std::size_t readableBytes();
    std::size_t freeBeforeSize();
    std::size_t readerIndex() const;
    void hasWritten(std::size_t len);
    const char* peek() const;
    void retrieve(std::size_t len);
    void retrieveAll();
    void EnsureFreeSpace(std::size_t len);
    void append(char*, std::size_t);
    const char* findCRLF();
    const char* find(const char*, std::size_t);

    std::vector<char>&& takeBuffer();

private:
    char* GetBasePointer();
    void Normalize();

    std::vector<char> buffer_;
    std::size_t readerIndex_;
    std::size_t writerIndex_;
};

// ============ BufferBlock =============
constexpr std::size_t BUFFERBLOCKSIZE = 8192;

struct BufferBlock {

    std::size_t write_index_;
    std::size_t read_index_;

    char buffer[BUFFERBLOCKSIZE];

    void reset();
    std::size_t readableBytes();
    std::size_t writableBytes();
    char* beginWrite();
    const char* peek() const;
    void hasWritten(std::size_t len);
    const char* find(const char* token, std::size_t token_len);
    const char* findCRLF();
    void retrieve(std::size_t len);
    void retrieveAll();
};

using BlockPtr = std::shared_ptr<BufferBlock>;

class BlockPool {

private:
    moodycamel::ConcurrentQueue<BufferBlock*> pool_;
    base::JemallocWrapper* mempool_;

public:
    BlockPool(base::JemallocWrapper* mempool);
    ~BlockPool();

    BlockPtr get();
    void release(BufferBlock* block);

};

};

};
