#include "Backlog.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace rkv {
ReplBacklog::ReplBacklog(uint64_t capacity) : capacity_(capacity) {
    if (capacity == 0) throw std::invalid_argument("replication backlog capacity must be positive");
    this->buffer_.resize(capacity);
}

uint64_t ReplBacklog::getMasterOffset() const
{ return this->master_offset_; }

uint64_t ReplBacklog::getCapacity() const
{ return this->capacity_; }

SendChunk ReplBacklog::getChunkToSend(uint64_t replica_offset) {
    if(replica_offset == this->master_offset_) return {nullptr, 0};
    if(replica_offset > this->master_offset_) return {nullptr, static_cast<std::size_t>(-1)};

    std::size_t unacked = this->master_offset_ - replica_offset;
    if(unacked > this->capacity_) {
        return {nullptr, static_cast<std::size_t>(-1)};
    }

    std::size_t index = replica_offset % this->capacity_;

    std::size_t send_len = std::min(this->capacity_ - index, unacked);
    return {this->buffer_.data() + index, send_len};
}

void ReplBacklog::append(const char* data, uint64_t len) {
    if (data == nullptr || len == 0) return;
    const uint64_t new_offset = master_offset_ + len;
    // Only the last capacity_ bytes can be retained.
    if (len > capacity_) {
        data += len - capacity_;
        master_offset_ += len - capacity_;
        len = capacity_;
    }
    const auto index = master_offset_ % capacity_;
    const auto first = std::min(len, capacity_ - index);
    std::memcpy(buffer_.data() + index, data, first);
    if (first < len) std::memcpy(buffer_.data(), data + first, len - first);
    master_offset_ = new_offset;
}

}
