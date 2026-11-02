#include "ShardRouter.h"
#include "net/EventLoop.h"

#include <cstdint>
#include <stdexcept>
#include <utility>

namespace rkv {
ShardRouter::ShardRouter(std::size_t count) : shards_(count) {
    if (count == 0) throw std::invalid_argument("ShardRouter requires at least one shard");
}

void ShardRouter::bind(std::size_t shard, AeroIO::net::EventLoop& loop, Ringengine& engine) {
    auto& entry = shards_.at(shard);
    if (entry.loop) throw std::logic_error("shard already bound");
    if (!loop.isInLoopThread()) throw std::logic_error("bind shard on its owner thread");
    entry = {&loop, &engine};
}

const ShardRouter::Shard& ShardRouter::at(std::size_t shard) const {
    const auto& entry = shards_.at(shard);
    if (!entry.loop || !entry.engine) throw std::logic_error("shard is not ready");
    return entry;
}

std::size_t ShardRouter::shardFor(std::string_view key) const noexcept {
    uint64_t hash = 0xcbf29ce484222325;
    for (char byte : key) {
        hash ^= byte;
        hash *= 0x100000001b3;
    }
    return hash % shards_.size();
}

bool ShardRouter::isLocal(std::size_t shard) const { return at(shard).loop->isInLoopThread(); }

void ShardRouter::dispatch(std::size_t shard, Task task) const {
    const auto& entry = at(shard);
    entry.loop->runInLoop([engine = entry.engine, task = std::move(task)]() mutable {
        task(*engine);
    });
}

void ShardRouter::broadcast(Task task) const {
    for (std::size_t shard = 0; shard < size(); ++shard) dispatch(shard, task);
}
}
