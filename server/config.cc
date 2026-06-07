#include "config.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace rkv {

ConfigBuilder::ConfigBuilder(int argc, char* argv[]) {
    for(int i = 1; i < argc; ++i) {
        std::string key = argv[i];

        if(key.starts_with("--")) {
            if(i + 1 >= argc) {
                throw std::runtime_error("missing value for " + key);
            }

            this->args_[key] = argv[++i];
        }
    }

    if(!this->args_.contains("--config")) {
        throw std::runtime_error("missing value for --config");
    }

    this->config_path_ = this->args_["--config"];
}

Config ConfigBuilder::Build() {
    std::ifstream ifs(this->config_path_);
    std::string line;

    Config config;

    while(std::getline(ifs, line)) {
        if(line.empty() || line[0] == '#' || line[0] == '\r') continue;

        std::istringstream iss(line);
        std::string key, value;

        if(iss >> key >> value) {
            if(key == "ip") config.ip_ = value;
            else if (key == "port") config.port_ = std::stoi(value);
            else if (key == "shard_threads") config.shard_threads_ = std::stoi(value);
            else if (key == "aof_enabled") config.aof_enabled_ = value == "yes";
            else if (key == "aof_sync") config.aof_sync_type_ = value == "everysec" ? 
                                        AofSyncType::EVERYSEC : AofSyncType::NO;
            else if (key == "rdb_enabled") config.rdb_enabled_ = value == "yes";
            else if (key == "rdb_interval") config.rdb_interval_ = std::stol(value);
            else if (key == "cluster_enabled") config.cluster_enabled_ = value == "yes";
            else if (key == "is_master") config.is_master_ = value == "yes";
            else if (key == "master_ip") config.master_ip_ = value;
            else if (key == "master_port") config.master_port_ = std::stoi(value);
        }
    }

    ApplyOverride(config);

    return config;
}

void ConfigBuilder::ApplyOverride(Config& config) {
    if(this->args_.contains("--port")) {
        config.port_ = std::stoi(this->args_["--port"]);
    }

    if(this->args_.contains("--threads")) {
        config.shard_threads_ = std::stoi(this->args_["--threads"]);
    }
}
    
};