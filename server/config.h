#pragma once

#include <string>
#include <unordered_map>

namespace rkv {

constexpr const char* ConfigFilePath = "../config/server.conf";

enum class AofSyncType {
    EVERYSEC, NO
};

struct Config {
    std::string ip_;
    int port_ = 5005;
    int shard_threads_;

    bool aof_enabled_;
    AofSyncType aof_sync_type_;

    bool rdb_enabled_;
    std::size_t rdb_interval_;

    bool cluster_enabled_;
    bool is_master_;
    std::string master_ip_;
    int master_port_;
};

class ConfigBuilder {

public:
    ConfigBuilder(int argc, char* argv[]);

    Config Build();
    void ApplyOverride(Config& config);

private:
    std::string config_path_;
    std::unordered_map<std::string, std::string> args_;

};

};