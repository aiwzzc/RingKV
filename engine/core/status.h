#pragma once

namespace rkv {
constexpr int KVS_OK = 0;
constexpr int KVS_ERR_NULL = -1;       // 内存分配失败
constexpr int KVS_ERR_TYPE = -2;       // 类型错误
constexpr int KVS_ERR_NOT_INT = -3;    // 无法解析为整数
constexpr int KVS_ERR_OVERFLOW = -4;   // 数值溢出/下溢

}
