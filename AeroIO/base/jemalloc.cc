#include "jemalloc.h"

#pragma push_macro("free")
#undef free

namespace rkv {

void* JemallocWrapper::alloc(std::size_t size) {
    void* ptr = ::je_malloc(size);
    if(ptr) {
        this->used_memory_.fetch_add(je_malloc_usable_size(ptr), std::memory_order_relaxed);
    }

    return ptr;
}

void JemallocWrapper::free(void* ptr) {
    if(!ptr) return;

    std::size_t actual_size = je_malloc_usable_size(ptr);
    ::je_free(ptr);

    this->used_memory_.fetch_sub(actual_size, std::memory_order_relaxed);
}

std::size_t JemallocWrapper::get_used_memory() const {
    return this->used_memory_.load(std::memory_order_relaxed);
}

};

#pragma pop_macro("free")