#pragma once

#include <unordered_map>
#include <string>
#include <vector>
#include <functional>
#include <optional>
#include <iostream>

#include "unordered_dense/include/ankerl/unordered_dense.h"

namespace rkv {

struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const {
        return std::hash<std::string_view>{}(sv);
    }
};

class JemallocWrapper;
struct RedisObject;
struct kvstr;

#if 1
template<typename Key, typename Value>
class rhash_sec {

public:
    rhash_sec() = default;
    ~rhash_sec() = default;

    int hash_set(std::string_view field, const Value& value) {
        auto it = this->hash_.find(field);

        if(it != this->hash_.end()) {
            it->second = value;
            return 1;
        }

        this->hash_.emplace(field, value);

        return 0;
    }

    std::optional<Value> hash_get(std::string_view field) {
        auto it = this->hash_.find(field);

        if(it != this->hash_.end()) {
            return it->second;
        }

        return std::nullopt;
    }

    int hash_del(std::string_view field) {
        auto it = hash_.find(field);
        if (it == hash_.end()) return 0;

        hash_.erase(it);
        return 1;
    }

    std::size_t hash_len() {
        return this->hash_.size();
    }

    bool hash_exists(std::string_view field) {
        return this->hash_.contains(field);
    }

    void flushall()
    { this->hash_.clear(); }

    std::vector<std::pair<Key, Value>> hash_get_all() {
        std::vector<std::pair<Key, Value>> res;
        res.reserve(this->hash_.size());

        for(const auto& [key, value] : this->hash_) {
            res.emplace_back(key, value);
        }

        return res;
    }
    // void hash_traverse();

    std::unordered_map<Key, Value, StringHash, std::equal_to<>>& getUnderlyingMap()
    { return this->hash_; }

private:
    std::unordered_map<Key, Value, StringHash, std::equal_to<>> hash_;

};
#endif

inline std::string_view kvstr_to_sv(const kvstr* s);

struct KvstrHash {
    using is_transparent = void;
    std::size_t operator()(const kvstr* s) const;
    std::size_t operator()(std::string_view sv) const;
};

struct KvstrEqual {
    using is_transparent = void;

    bool operator()(const kvstr* a, const kvstr* b) const;
    bool operator()(const kvstr* a, std::string_view b) const;
    bool operator()(std::string_view a, const kvstr* b) const;
};

template<typename Value>
class rhash {

private:
    using MapType = ankerl::unordered_dense::map<kvstr*, Value, KvstrHash, KvstrEqual>;
    MapType hash_;

public:
    rhash() = default;
    ~rhash() = default;

    std::optional<Value> hash_get(std::string_view field) {
        auto it = this->hash_.find(field);

        if(it != this->hash_.end()) return it->second;

        return std::nullopt;
    }

    bool hash_exists(std::string_view field) {
        return this->hash_.find(field) != this->hash_.end();
    }

    typename MapType::iterator find(std::string_view field) {
        return this->hash_.find(field);
    }

    typename MapType::iterator end() {
        return this->hash_.end();
    }

    void emplace(kvstr* allocated_key, const Value& value) {
        this->hash_.emplace(allocated_key, value);
    }

    std::size_t hash_len() {
        return this->hash_.size();
    }

    void flushall()
    { this->hash_.clear(); }

    MapType& getUnderlyingMap()
    { return this->hash_; }

    int hash_del(std::string_view field) {
        auto it = hash_.find(field);
        if (it == hash_.end()) return 0;

        hash_.erase(it);
        return 1;
    }

};

class HashObject {

private:
    rhash<kvstr*> hash_;

public:
    static RedisObject* CreateHashObject(JemallocWrapper* mempool);

    int hash_set(std::string_view field, std::string_view value);
    std::optional<kvstr*> hash_get(std::string_view field);
    int hash_del(std::string_view field);
    std::size_t hash_len();
    bool hash_exists(std::string_view field);
    std::vector<std::pair<kvstr*, kvstr*>> hash_get_all();
    
};


};