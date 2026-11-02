#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rkv {
struct SendChunk {

    const char* data_;
    std::size_t len_;

};

class ReplBacklog {

private:
    std::vector<char> buffer_;
    uint64_t master_offset_ = 0;
    uint64_t capacity_;

public:
    ReplBacklog(uint64_t capacity = 10 * 1024 * 1024);

    uint64_t getMasterOffset() const;
    uint64_t getCapacity() const;

    SendChunk getChunkToSend(uint64_t offset);
    void append(const char* data, uint64_t len);

};

}
