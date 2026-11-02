#include "writer.h"
#include "ds/kvstr.h"
#include "ds/robject.h"

#include <cstdint>
#include <limits>
#include <stdexcept>

namespace rkv {
namespace {
std::string stringValue(const RedisObject& object) {
    if (object.type != OBJ_STRING) throw std::invalid_argument("snapshot encoder supports strings only");
    if (object.encoding == OBJ_ENCODING_INT)
        return std::to_string(reinterpret_cast<std::intptr_t>(object.ptr));
    const auto& value = *static_cast<const kvstr*>(object.ptr);
    return std::string(value.buf_, value.len_);
}
void appendBulk(std::string& output, std::string_view value) {
    output += '$';
    output += std::to_string(value.size());
    output += "\r\n";
    output.append(value);
    output += "\r\n";
}
void appendBinaryString(std::string& output, std::string_view value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max())
        throw std::length_error("snapshot string exceeds uint32 length");
    const auto size = static_cast<std::uint32_t>(value.size());
    output.append(reinterpret_cast<const char*>(&size), sizeof(size));
    output.append(value);
}
}

std::string dumpObjectToResp(std::string_view key, const RedisObject& object) {
    const auto value = stringValue(object);
    std::string output = "*3\r\n";
    appendBulk(output, "SET");
    appendBulk(output, key);
    appendBulk(output, value);
    return output;
}

std::string dumpObjectToBinaryRdb(std::string_view key, const RedisObject& object) {
    const auto value = stringValue(object);
    std::string output(1, static_cast<char>(object.type));
    appendBinaryString(output, key);
    appendBinaryString(output, value);
    return output;
}
}
