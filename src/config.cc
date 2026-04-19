#include "config.h"
#include <fstream>
#include <sstream>
#include "jemalloc.h"

namespace rkv {

void Config::configParser(const char* filename) {
    std::ifstream ifs(filename);
    std::string line;

    while(std::getline(ifs, line)) {
        if(line.empty() || line[0] == '#' || line[0] == '\r') continue;

        std::istringstream iss(line);
        std::string key, value;

        if(iss >> key >> value) {
            if(key == "ip") this->ip_ = value;
            else if (key == "port") this->port_ = std::stoi(value);
            else if (key == "worker_threads") this->worker_threads_ = std::stoi(value);
            else if (key == "aof_enabled") this->aof_enabled_ = value == "yes";
            else if (key == "aof_sync") this->aof_sync_type_ = value == "everysec" ? 
                                        AofSyncType::EVERYSEC : AofSyncType::NO;
            else if (key == "rdb_enabled") this->rdb_enabled_ = value == "yes";
            else if (key == "rdb_interval") this->rdb_interval_ = std::stol(value);
            else if (key == "cluster_enabled") this->cluster_enabled_ = value == "yes";
            else if (key == "is_master") this->is_master_ = value == "yes";
            else if (key == "master_ip") this->master_ip_ = value;
            else if (key == "master_port") this->master_port_ = std::stoi(value);
            else if (key == "http_enable") this->http_enable_ = value == "yes";
            else if (key == "httpServer_port") this->httpServer_port = std::stoi(value);
        }
    }
}
    
};