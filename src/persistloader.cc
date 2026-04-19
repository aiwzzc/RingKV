#include "persistloader.h"
#include "engine.h"
#include "AeroIO/net/EventLoop.h"
#include "config.h"
#include <sys/mman.h>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

namespace {

    inline uint64_t stable_hash(std::string_view key) {
        uint64_t hash = 0xcbf29ce484222325;
        for (char c : key) {
            hash ^= c;
            hash *= 0x100000001b3;
        }
        return hash;
    }
    
};

using AeroIO::net::EventLoop;

namespace rkv {

AofLoader::AofLoader(LoaderManager* LoaderManager) : 
LoaderManager_(LoaderManager) {}

int AofLoader::load_from_aof(const char* filename, std::size_t& filesize) {
    if(filename == nullptr || this->LoaderManager_ == nullptr) return -1;

    int fd = ::open(filename , O_RDONLY);
    if(fd < 0) return -2;

    struct stat st;
    if(::fstat(fd, &st) < 0) { ::close(fd); return -3; }

    size_t fsize = st.st_size;
    if(fsize == 0) { ::close(fd); return 0; }
    filesize = fsize;

    char* data = (char*)mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
    if(data == MAP_FAILED) { ::close(fd); return -4; }

    char* begin = data;
    char* end = data + fsize;

    std::vector<std::string_view> args;
    while(begin < end) {
        if(*begin != '*') {
            begin++;
            continue;
        }

        begin++;

        char* line_end = (char*)memchr(begin, '\r', end - begin);
        if(line_end == nullptr) break;

        long argc = strtol(begin, nullptr, 10);
        begin = line_end + 2;

        args.clear();
        args.reserve(argc);

        for(int i = 0; i < argc; ++i) {
            if(begin >= end || *begin != '$') break;
            begin++;

            line_end = (char*)memchr(begin, '\r', end - begin);
            if(line_end == nullptr) break;

            long len = strtol(begin, nullptr, 10);
            begin = line_end + 2;

            if(begin + len > end) break;

            args.emplace_back(begin, len);

            begin += len + 2;
            
        }

        if(!args.empty()) {
            std::string cmd(args[0]);

            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

            auto it = this->LoaderManager_->hashCallbacks_.find(cmd);
            if(it != this->LoaderManager_->hashCallbacks_.end()) {
                it->second(args);

            } else {
                std::cerr << "Unknown command in AOF: " << cmd << std::endl;

            }
        }
    }

    munmap(data, fsize);
    close(fd);

    return 0;
}

RdbLoader::RdbLoader(rkv::LoaderManager* LoaderManager)
{ this->LoaderManager_ = LoaderManager; }

int RdbLoader::load_from_rdb(const char* filename) {
    if(filename == nullptr || this->LoaderManager_ == nullptr) return -1;

    int fd = open(filename , O_RDONLY);
    if(fd < 0) return -2;

    struct stat st;
    if(fstat(fd, &st) < 0) { ::close(fd); return -3; }

    size_t fsize = st.st_size;
    if(fsize == 0) { close(fd); return 0; }

    char* data = (char*)mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
    if(data == MAP_FAILED) { close(fd); return -4; }

    char* begin = data;
    char* end = data + fsize;

    while(begin < end) {
        if(begin + sizeof(uint32_t) > end) break;

        uint8_t type_flag = *reinterpret_cast<uint8_t*>(begin);
        begin += sizeof(uint8_t);

        if(type_flag == OBJ_STRING) {
            uint32_t key_len = *reinterpret_cast<uint32_t*>(begin);
            begin += sizeof(uint32_t);

            if(begin + key_len > end) break;

            std::string_view key(begin, key_len);
            begin += key_len;

            if(begin + sizeof(uint32_t) > end) break;

            uint32_t value_len = *reinterpret_cast<uint32_t*>(begin);
            begin += sizeof(uint32_t);

            if(begin + value_len > end) break;

            std::string_view value(begin, value_len);
            begin += value_len;

            std::vector<std::string_view> args = { "SET", key, value };
            this->LoaderManager_->hashCallbacks_["SET"](args);

        } else if(type_flag == OBJ_LIST) {
            uint32_t key_len = *reinterpret_cast<uint32_t*>(begin);
            begin += sizeof(uint32_t);

            if(begin + key_len > end) break;

            uint32_t value_num = *reinterpret_cast<uint32_t*>(begin);
            begin += sizeof(uint32_t);

            if(begin + sizeof(uint32_t) > end) break;

            std::vector<std::string> value_vec;

            for(int i = 0; i < value_num; ++i) {
                uint32_t value_len = *reinterpret_cast<uint32_t*>(begin);
                begin += sizeof(uint32_t);

                value_vec.emplace_back(begin, value_len);
                begin += value_len;
            }
        }
    }

    munmap(data, fsize);
    close(fd);

    return 0;
}

LoaderManager::LoaderManager(Ringengine* engine, LoopsEngines LoopsEngines) :
engine_(engine), LoopsEngines_(LoopsEngines) {

    this->hashCallbacks_ = {
        {
            "SET",
            [this] (const std::vector<std::string_view>& args) {
                if(args.size() == 3) {
                    std::size_t target_index = stable_hash(args[1]) % this->LoopsEngines_->size();
                    EventLoop* target_loop = (*this->LoopsEngines_)[target_index].first;
                    Ringengine* target_engine = (*this->LoopsEngines_)[target_index].second;
                    EventLoop* curr_loop = EventLoop::getEventLoopOfCurrentThread();

                    if(target_loop == curr_loop) {
                        this->engine_->set(args[1], args[2]);

                    } else {
                        std::vector<std::string> cmds(args.begin(), args.end());
                        target_loop->runInLoop([target_engine, cmds = std::move(cmds)] () {
                            target_engine->set(cmds[1], cmds[2]);
                        });
                    }
                }
            }
        },
        {
            "LPUSH",
            [this] (const std::vector<std::string_view>& args) {
                
            }
        }
    };
}

void LoaderManager::start() {
    if(this->LoopsEngines_ == nullptr) return;

    this->aofLoader_ = std::make_unique<AofLoader>(this);
    this->rdbLoader_ = std::make_unique<RdbLoader>(this);
    EventLoop* curr_loop = nullptr;

    for(auto& [loop, engine] : *this->LoopsEngines_) {
        if(engine == this->engine_) curr_loop = loop;
    }

    if(curr_loop == nullptr || curr_loop->getAofFilePath() == nullptr ||
        curr_loop->getRdbFilePath() == nullptr) return;

    if(Config::getInstance().aof_enabled_) {
        this->aofLoader_->load_from_aof(curr_loop->getAofFilePath(), curr_loop->getAofFileOffset());
    }

    if(Config::getInstance().rdb_enabled_) {
        this->rdbLoader_->load_from_rdb(curr_loop->getRdbFilePath());
    }
}

};