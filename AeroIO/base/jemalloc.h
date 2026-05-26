#pragma once

#include <atomic>

#define JEMALLOC_NO_DEMANGLE 1
#include <jemalloc/jemalloc.h>

namespace rkv {

class JemallocWrapper {

private:
    std::atomic<std::size_t> used_memory_{0};

public:

    void* alloc(std::size_t size);
    void free(void* ptr);
    std::size_t get_used_memory() const;
};

};