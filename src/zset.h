#pragma once

#include <unordered_map>
#include <set>
#include <string>
#include <vector>
#include "dict.h"
#include "kvstr.h"

namespace rkv {

class JemallocWrapper;

class ZSetObject {

private:
    struct ZNode {
        double socre_;
        kvstr* member_;
    };

    struct Cmp {
        bool operator()(const ZNode* a, const ZNode* b) const {
            if(a->socre_ != b->socre_) return a->socre_ < b->socre_;

            return a->member_->buf_ < b->member_->buf_;
        }
    };

    std::set<ZNode*, Cmp> tree_;
    rhash<ZNode*> dict_;

public:
    static RedisObject* CreateZSetObject(JemallocWrapper* mempool);
    void release_all_node(JemallocWrapper* mempool);

    int zset_add(double socre, std::string_view member, JemallocWrapper* mempool);
    std::vector<kvstr*> zset_revrange(int start, int end);
    std::vector<kvstr*> zset_range(int start, int end);
    std::optional<double> zset_score(std::string_view member);
    int zset_card();
    int zset_rank(std::string_view member);
    int zset_revrank(std::string_view member);
};

};