#pragma once

#include <string>
#include <memory>

namespace rkv {

constexpr const char* ConfigFilePath = "../config/server.conf";

enum class AofSyncType {
    EVERYSEC, NO
};

class Config {

public:
    Config() = default;

    static Config& getInstance() {
        static Config instance;
        return instance;
    }

    Config(const Config&) = delete;
    Config& operator=(const Config&) = delete;

public:
    std::string ip_;
    int port_ = 5005;
    int worker_threads_;

    bool aof_enabled_;
    AofSyncType aof_sync_type_;

    bool rdb_enabled_;
    std::size_t rdb_interval_;

    bool cluster_enabled_;
    bool is_master_;
    std::string master_ip_;
    int master_port_;

    bool http_enable_;
    int httpServer_port = 8080;

    void configParser(const char* filename = ConfigFilePath);
};

};