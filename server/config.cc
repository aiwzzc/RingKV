#include "config.h"

#include <arpa/inet.h>
#include <charconv>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>

namespace rkv {
namespace {
int number(const std::string& key, const std::string& value) {
    int result{};
    auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (ec != std::errc{} || end != value.data() + value.size())
        throw std::runtime_error(key + ": expected an integer, got '" + value + "'");
    return result;
}

bool boolean(const std::string& key, const std::string& value) {
    if (value == "yes") return true;
    if (value == "no") return false;
    throw std::runtime_error(key + ": expected yes or no");
}

void apply(Config& config, const std::string& key, const std::string& value) {
    if (key == "ip") config.ip_ = value;
    else if (key == "port") config.port_ = number(key, value);
    else if (key == "shard_threads") config.shard_threads_ = number(key, value);
    else if (key == "aof_enabled") config.aof_enabled_ = boolean(key, value);
    else if (key == "aof_sync") {
        if (value != "everysec" && value != "no")
            throw std::runtime_error("aof_sync: expected everysec or no");
        config.aof_sync_type_ = value == "everysec" ? AofSyncType::EVERYSEC : AofSyncType::NO;
    } else if (key == "rdb_enabled") config.rdb_enabled_ = boolean(key, value);
    else if (key == "rdb_interval") {
        auto interval = number(key, value);
        if (interval <= 0) throw std::runtime_error("rdb_interval must be positive");
        config.rdb_interval_ = interval;
    } else if (key == "cluster_enabled") config.cluster_enabled_ = boolean(key, value);
    else if (key == "is_master") config.is_master_ = boolean(key, value);
    else if (key == "master_ip") config.master_ip_ = value;
    else if (key == "master_port") config.master_port_ = number(key, value);
    else throw std::runtime_error("unknown configuration key: " + key);
}
}

void Config::validate() const {
    in_addr address{};
    if (inet_pton(AF_INET, ip_.c_str(), &address) != 1)
        throw std::runtime_error("ip must be an IPv4 address");
    if (inet_pton(AF_INET, master_ip_.c_str(), &address) != 1)
        throw std::runtime_error("master_ip must be an IPv4 address");
    if (port_ < 1 || port_ > 65535 || master_port_ < 1 || master_port_ > 65535)
        throw std::runtime_error("port and master_port must be in [1, 65535]");
    if (shard_threads_ < 1 || shard_threads_ > 256)
        throw std::runtime_error("shard_threads must be in [1, 256]");
    if (rdb_interval_ == 0) throw std::runtime_error("rdb_interval must be positive");
    // These paths are unfinished in the current engine. Never silently promise durability.
    if (aof_enabled_ || rdb_enabled_ || cluster_enabled_ || !is_master_)
        throw std::runtime_error("AOF, RDB and replication are not supported by this build; use standalone memory mode");
}

std::string Config::dump() const {
    auto yesno = [](bool value) { return value ? "yes" : "no"; };
    std::ostringstream out;
    out << "ip " << ip_ << "\nport " << port_ << "\nshard_threads " << shard_threads_
        << "\naof_enabled " << yesno(aof_enabled_)
        << "\naof_sync " << (aof_sync_type_ == AofSyncType::EVERYSEC ? "everysec" : "no")
        << "\nrdb_enabled " << yesno(rdb_enabled_) << "\nrdb_interval " << rdb_interval_
        << "\ncluster_enabled " << yesno(cluster_enabled_) << "\nis_master " << yesno(is_master_)
        << "\nmaster_ip " << master_ip_ << "\nmaster_port " << master_port_ << '\n';
    return out.str();
}

ConfigBuilder::ConfigBuilder(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (key == "--help" || key == "-h") { help_ = true; continue; }
        if (key == "--check-config") { check_only_ = true; continue; }
        if (key == "--dump-config") { dump_only_ = true; continue; }
        if (key != "--config" && key != "--port" && key != "--threads" && key != "--ip")
            throw std::runtime_error("unknown option: " + key);
        if (args_.contains(key)) throw std::runtime_error("duplicate option: " + key);
        if (i + 1 >= argc || std::string(argv[i + 1]).starts_with("--"))
            throw std::runtime_error("missing value for " + key);
        args_.emplace(key, argv[++i]);
    }
    if (args_.contains("--config")) config_path_ = args_.at("--config");
}

Config ConfigBuilder::Build() {
    Config config{};
    if (!config_path_.empty()) {
        std::ifstream input(config_path_);
        if (!input) throw std::runtime_error("cannot open configuration: " + config_path_);
        std::set<std::string> seen;
        std::string line;
        std::size_t line_number = 0;
        while (std::getline(input, line)) {
            ++line_number;
            if (auto comment = line.find('#'); comment != std::string::npos) line.erase(comment);
            std::istringstream fields(line);
            std::string key, value, extra;
            if (!(fields >> key)) continue;
            try {
                if (!(fields >> value) || (fields >> extra))
                    throw std::runtime_error("expected one key and one value");
                if (!seen.insert(key).second) throw std::runtime_error("duplicate key: " + key);
                apply(config, key, value);
            } catch (const std::exception& error) {
                throw std::runtime_error(config_path_ + ":" + std::to_string(line_number) + ": " + error.what());
            }
        }
        if (input.bad()) throw std::runtime_error("error reading configuration: " + config_path_);
    }
    ApplyOverride(config);
    config.validate();
    return config;
}

void ConfigBuilder::ApplyOverride(Config& config) {
    if (args_.contains("--port")) apply(config, "port", args_.at("--port"));
    if (args_.contains("--threads")) apply(config, "shard_threads", args_.at("--threads"));
    if (args_.contains("--ip")) apply(config, "ip", args_.at("--ip"));
}
}
