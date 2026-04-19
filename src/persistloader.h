#pragma once

#include <unordered_map>
#include <functional>
#include <string>
#include <memory>
#include <vector>

namespace AeroIO {

namespace net {

class EventLoop;

};

};

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
    using LoopsEngines = std::vector<std::pair<AeroIO::net::EventLoop*, Ringengine*>>*;

    explicit LoaderManager(Ringengine* engine, LoopsEngines LoopsEngines);
    
    void start();

private:
    Ringengine* engine_;
    LoopsEngines LoopsEngines_;
    std::unique_ptr<AofLoader> aofLoader_;
    std::unique_ptr<RdbLoader> rdbLoader_;
    HashCallbacks hashCallbacks_;

};

    
};