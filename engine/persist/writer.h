#pragma once

#include <string>
#include <string_view>

namespace rkv {
struct RedisObject;

// Storage encoders own the output; no network buffer or connection is involved.
std::string dumpObjectToResp(std::string_view key, const RedisObject& object);
std::string dumpObjectToBinaryRdb(std::string_view key, const RedisObject& object);
}
