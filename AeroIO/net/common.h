#pragma once

#include <vector>
#include <string_view>
#include <cstdint>
#include <variant>
#include <memory>

namespace rkv {

struct CommandDef;

};

namespace AeroIO {

namespace net {

struct BufferBlock;
using BlockPtr = std::shared_ptr<BufferBlock>;

using Args = std::variant<BlockPtr, std::string>;

struct RoutedCommand {
    std::vector<std::string_view> tokens_;
    uint64_t slot_id_;
    Args buffer_;
    const rkv::CommandDef* cmd_def_;
    std::string response_data_;
};

using RouteBatch = std::vector<RoutedCommand>;

};

};