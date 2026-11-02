#pragma once

#include <cstddef>
#include <functional>
#include <string_view>
#include <vector>

namespace AeroIO::net { class EventLoop; }

namespace rkv {
class Ringengine;

// Bound by each owner during prepare(); read-only after the startup gate opens.
class ShardRouter {
public:
    using Task = std::function<void(Ringengine&)>;

    explicit ShardRouter(std::size_t count);
    void bind(std::size_t shard, AeroIO::net::EventLoop& loop, Ringengine& engine);
    std::size_t size() const noexcept { return shards_.size(); }
    std::size_t shardFor(std::string_view key) const noexcept;
    bool isLocal(std::size_t shard) const;
    void dispatch(std::size_t shard, Task task) const;
    void broadcast(Task task) const;

private:
    struct Shard {
        AeroIO::net::EventLoop* loop = nullptr;
        Ringengine* engine = nullptr;
    };
    const Shard& at(std::size_t shard) const;
    std::vector<Shard> shards_;
};
}
