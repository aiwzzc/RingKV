#include "list.h"
#include "jemalloc.h"
#include "kvstr.h"
#include <string.h>

namespace rkv {

RedisObject* ListObject::CreateListObject(JemallocWrapper* mempool) {
    if(mempool == nullptr) return nullptr;

    RedisObject* obj = (RedisObject*)mempool->alloc(sizeof(RedisObject));
    if(obj == nullptr) return nullptr;

    obj->len = 0;
    obj->refcount = 1;
    obj->type = OBJ_LIST;

    ListObject* list_obj = (ListObject*)mempool->alloc(sizeof(ListObject));
    if(list_obj == nullptr) return nullptr;

    new (list_obj) ListObject(mempool);
    obj->ptr = list_obj;

    return obj;
}

std::size_t ListObject::getlen() const { return this->len_; }

ListObject::ListObject(JemallocWrapper* mempool) : len_(0) {
    this->head_ = (ListNode*)mempool->alloc(sizeof(ListNode));
    this->tail_ = (ListNode*)mempool->alloc(sizeof(ListNode));

    ::memset(this->head_, 0, sizeof(ListNode));
    ::memset(this->tail_, 0, sizeof(ListNode));

    this->head_->next_ = this->tail_;
    this->tail_->prev_ = this->head_;
}
ListObject::~ListObject() = default;

void ListObject::release_all_node(JemallocWrapper* mempool) {
    if(mempool == nullptr) return;

    ListNode* currnode = this->head_->next_;
    while(currnode && currnode != this->tail_) {
        ListNode* nextnode = currnode->next_;

        // decrRefCount(currnode->obj_, mempool);

        mempool->free(currnode->obj_);
        mempool->free(currnode);
        currnode = nextnode;
    }

    mempool->free(this->head_);
    mempool->free(this->tail_);
    this->len_ = 0;
}

int ListObject::list_push_head(kvstr* obj, JemallocWrapper* mempool) {
    if(obj == nullptr || mempool == nullptr) return -1;

    ListNode* node = (ListNode*)mempool->alloc(sizeof(ListNode));
    if(node == nullptr) return -2;

    node->obj_ = obj;

    node->next_ = this->head_->next_;
    node->prev_ = this->head_;
    this->head_->next_->prev_ = node;
    this->head_->next_ = node;

    ++this->len_;

    return this->len_;
}

int ListObject::list_push_tail(kvstr* obj, JemallocWrapper* mempool) {
    if(obj == nullptr || mempool == nullptr) return -1;

    ListNode* node = (ListNode*)mempool->alloc(sizeof(ListNode));
    if(node == nullptr) return -2;

    node->obj_ = obj;

    node->next_ = this->tail_;
    node->prev_ = this->tail_->prev_;
    this->tail_->prev_->next_ = node;
    this->tail_->prev_ = node;

    ++this->len_;

    return this->len_;
}

kvstr* ListObject::list_pop_head(JemallocWrapper* mempool) {
    if(mempool == nullptr || this->len_ <= 0) return nullptr;

    ListNode* node = this->head_->next_;

    kvstr* obj = node->obj_;

    this->head_->next_ = node->next_;
    node->next_->prev_ = this->head_;
    this->len_--;

    mempool->free(node);
    
    return obj;
}

kvstr* ListObject::list_pop_tail(JemallocWrapper* mempool) {
    if(mempool == nullptr || this->len_ <= 0) return nullptr;

    ListNode* node = this->tail_->prev_;

    kvstr* obj = node->obj_;

    this->tail_->prev_ = node->prev_;
    node->prev_->next_ = this->tail_;
    this->len_--;

    mempool->free(node);

    return obj;
}

std::vector<kvstr*> ListObject::list_range(int start, int stop) {
    std::vector<kvstr*> result;

    if(this->len_ == 0) return result;

    long long llen = static_cast<long long>(this->len_);

    // 负数索引
    if(start < 0) start = llen + start;
    if(stop < 0) stop = llen + stop;

    if(start < 0) start = 0;
    if(start > stop || start > llen) return result;
    if(stop >= llen) stop = llen - 1;

    ListNode* curnode = this->head_->next_;
    int idx{0};
    while(idx < start && curnode != this->tail_) {
        curnode = curnode->next_;
        idx++;
    }

    while(idx <= stop && curnode != this->tail_) {
        result.push_back(curnode->obj_);
        // incrRefCount(curnode->obj_);

        curnode = curnode->next_;
        idx++;
    }

    return result;
}

kvstr* ListObject::list_index(int index) {
    index = index < 0 ? this->len_ + index : index;
    if(index < 0) return nullptr;

    if(index >= this->len_) return nullptr;

    ListNode* curnode = this->head_->next_;
    while(index-- && curnode != this->tail_) curnode = curnode->next_;
    
    kvstr* obj = curnode->obj_;

    return obj;
}

int ListObject::list_modify_index(int index, std::string_view data, JemallocWrapper* mempool) {
    index = index < 0 ? this->len_ + index : index;
    if(index < 0 || index >= this->len_) return -3;

    ListNode* curnode = this->head_->next_;
    while(index-- && curnode != this->tail_) curnode = curnode->next_;

    kvstr* s = curnode->obj_;
    mempool->free(s);

    s = kvstr::KvstrCreate(data.data(), data.size(), mempool);
    curnode->obj_ = s;

    // if(curnode->obj_->type == OBJ_STRING) {
    //     if(curnode->obj_->encoding == OBJ_ENCODING_RAW) {
    //         kvstr* s = (kvstr*)curnode->obj_->ptr;
    //         mempool->free(s);

    //         s = kvstr::KvstrCreate(data.data(), data.size(), mempool);
    //         curnode->obj_->ptr = (void*)s;
    //         curnode->obj_->len = data.size();

    //     } else if (curnode->obj_->encoding == OBJ_ENCODING_INT) {
    //         kvstr* s = kvstr::KvstrCreate(data.data(), data.size(), mempool);
    //         curnode->obj_->ptr = (void*)s;
    //         curnode->obj_->len = data.size();
    //         curnode->obj_->encoding = OBJ_ENCODING_RAW;
    //     }
    // }

    return 1;
}

};