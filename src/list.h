#pragma once

#include "common.h"
#include <vector>
#include <string_view>

namespace rkv {

class JemallocWrapper;
struct kvstr;

struct ListNode {
    ListNode* next_;
    ListNode* prev_;
    kvstr* obj_;
};

class ListObject {

public:
    explicit ListObject(JemallocWrapper* mempool);
    ~ListObject();

    static RedisObject* CreateListObject(JemallocWrapper* mempool);

    std::size_t getlen() const;
    void release_all_node(JemallocWrapper* mempool);
    int list_push_head(kvstr* obj, JemallocWrapper* mempool);                                     // LPUSH
    int list_push_tail(kvstr* obj, JemallocWrapper* mempool);                                     // RPUSH
    int list_modify_index(int index, std::string_view data, JemallocWrapper* mempool);                  // LSET
    kvstr* list_pop_head(JemallocWrapper* mempool);                                               // LPOP
    kvstr* list_pop_tail(JemallocWrapper* mempool);                                               // RPOP
    kvstr* list_index(int index);                                                                 // LINDEX
    std::vector<kvstr*> list_range(int start, int stop);                                          // LRANGE

private:
    ListNode* head_;
    ListNode* tail_;
    std::size_t len_;
};

};