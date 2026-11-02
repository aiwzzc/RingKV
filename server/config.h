#pragma once

#include <string>
#include <unordered_map>

namespace rkv {

enum class AofSyncType {
    EVERYSEC, NO
};

struct Config {
    std::string ip_ = "127.0.0.1";
    int port_ = 5005;
    int shard_threads_ = 1;

    bool aof_enabled_ = false;
    AofSyncType aof_sync_type_ = AofSyncType::EVERYSEC;

    bool rdb_enabled_ = false;
    std::size_t rdb_interval_ = 60;

    bool cluster_enabled_ = false;
    bool is_master_ = true;
    std::string master_ip_ = "127.0.0.1";
    int master_port_ = 5005;

    void validate() const;
    std::string dump() const;
};

class ConfigBuilder {

public:
    ConfigBuilder(int argc, char* argv[]);

    Config Build();
    bool checkOnly() const { return check_only_; }
    bool dumpOnly() const { return dump_only_; }
    bool help() const { return help_; }

private:
    void ApplyOverride(Config& config);
    std::string config_path_;
    std::unordered_map<std::string, std::string> args_;
    bool check_only_ = false;
    bool dump_only_ = false;
    bool help_ = false;

};

};
