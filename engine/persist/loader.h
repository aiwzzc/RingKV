#pragma once

#include <unordered_map>
#include <functional>
#include <memory>
#include <vector>
#include <string_view>

namespace rkv {

class Ringengine;
class LoaderManager;
struct Config;

class AofLoader {

public:
    explicit AofLoader(rkv::LoaderManager* LoaderManager);

    int load_from_aof(const char* filename, std::size_t& filesize);

private:
    LoaderManager* LoaderManager_;

};

class RdbLoader {

public:
    explicit RdbLoader(rkv::LoaderManager* LoaderManager);

    int load_from_rdb(const char* filename);

private:
    LoaderManager* LoaderManager_;

};

class LoaderManager {

public:
    friend class AofLoader;
    friend class RdbLoader;

    using HashCallbacks = std::unordered_map<std::string_view, std::function<void(const std::vector<std::string_view>&)>>;

    explicit LoaderManager(Ringengine* engine);
    
    void start();

private:
    Ringengine* engine_;
    std::unique_ptr<AofLoader> aofLoader_;
    std::unique_ptr<RdbLoader> rdbLoader_;
    HashCallbacks hashCallbacks_;

};

    
};