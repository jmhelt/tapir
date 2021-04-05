
#include "store/strongstore/networkconfig.h"

#include <algorithm>
#include <string>
#include <vector>

#include "lib/assert.h"

namespace strongstore {

NetworkConfiguration::NetworkConfiguration(
    transport::Configuration &tport_config, std::istream &file)
    : tport_config_{tport_config} {
    file >> net_config_json_;
}

NetworkConfiguration::~NetworkConfiguration() {}

const std::string &NetworkConfiguration::GetRegion(
    const std::string &host) const {
    Debug("GetRegion: %s", host.c_str());

    for (auto &kv : net_config_json_["server_regions"].items()) {
        const std::string &region = kv.key();
        const std::vector<std::string> &hosts =
            kv.value().get<std::vector<std::string>>();
        for (const std::string &h : hosts) {
            if (h == host) {
                Debug("GetRegion: %s", region.c_str());
                return region;
            }
        }
    }

    NOT_REACHABLE();
    return INVALID_REGION;
}

const std::string &NetworkConfiguration::GetRegion(int shard_idx,
                                                   int replica_idx) const {
    Debug("GetRegion: %d %d", shard_idx, replica_idx);
    const std::string &host =
        tport_config_.replica(shard_idx, replica_idx).host;

    return GetRegion(host);
}

uint16_t NetworkConfiguration::GetOneWayLatency(
    const std::string &src_region, const std::string &dst_region) const {
    Debug("GetOneWayLatency: %s %s", src_region.c_str(), dst_region.c_str());
    uint16_t rtt =
        net_config_json_["region_rtt_latencies"][src_region][dst_region]
            .get<uint16_t>();

    Debug("GetOneWayLatency: %u", rtt / 2);
    return rtt / 2;
}

uint16_t NetworkConfiguration::GetMinQuorumLatency(int shard_idx,
                                                   int leader_idx) const {
    Debug("GetMinQuorumLatency: %d %d", shard_idx, leader_idx);
    int q = tport_config_.QuorumSize();
    int n = tport_config_.n;

    const std::string &leader_region = GetRegion(shard_idx, leader_idx);

    std::vector<uint16_t> lats;
    for (int i = 0; i < n; i++) {
        const std::string &replica_region = GetRegion(shard_idx, i);
        uint16_t rtt = net_config_json_["region_rtt_latencies"][leader_region]
                                       [replica_region]
                                           .get<uint16_t>();
        lats.push_back(rtt);
    }

    std::sort(lats.begin(), lats.end());

    Debug("GetMinQuorumLatency: %u", lats[q - 1]);
    return lats[q - 1];
}

}  // namespace strongstore