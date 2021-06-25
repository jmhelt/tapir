#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <bitset>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/networkconfig.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"
#include "store/strongstore/viewfinder.h"

namespace strongstore {

class Client : public ::Client {
   public:
    Client(Consistency consistency, const NetworkConfiguration &net_config,
           const std::string &client_region, transport::Configuration &config,
           uint64_t id, int nshards, int closestReplic, Transport *transport,
           Partitioner *part, TrueTime &tt, bool debug_stats,
           double nb_time_alpha);
    virtual ~Client();

    // Overriding functions from ::Client
    // Begin a transaction.
    virtual void Begin(bool is_retry, begin_callback bcb,
                       begin_timeout_callback btcb, uint32_t timeout) override;

    // Get the value corresponding to key.
    virtual void Get(const std::string &key, get_callback gcb,
                     get_timeout_callback gtcb,
                     uint32_t timeout = GET_TIMEOUT) override;

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    virtual void GetForUpdate(const std::string &key, get_callback gcb,
                              get_timeout_callback gtcb,
                              uint32_t timeout = GET_TIMEOUT) override;

    // Set the value for the given key.
    virtual void Put(const std::string &key, const std::string &value,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout = PUT_TIMEOUT) override;

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
                        uint32_t timeout) override;

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
                       uint32_t timeout) override;

    // Commit all Get(s) and Put(s) since Begin().
    void ROCommit(const std::unordered_set<std::string> &keys,
                  commit_callback ccb, commit_timeout_callback ctcb,
                  uint32_t timeout) override;

   private:
    const static std::size_t MAX_SHARDS = 16;

    enum State {
        EXECUTING,
        COMMITTING,
        ABORTED
    };

    struct PendingRequest {
        PendingRequest(uint64_t id, uint64_t txnId)
            : id(id),
              txnId(txnId),
              outstandingPrepares(0) {}

        ~PendingRequest() {}

        commit_callback ccb;
        commit_timeout_callback ctcb;
        abort_callback acb;
        abort_timeout_callback atcb;
        uint64_t id;
        uint64_t txnId;
        int outstandingPrepares;
    };

    // local Prepare function
    void Prepare(PendingRequest *req, uint32_t timeout);
    void CommitCallback(uint64_t reqId, int status, Timestamp commit_ts, Timestamp nonblock_ts);

    void AbortCallback(uint64_t reqId);

    void ROCommitCallback(uint64_t transaction_id, uint64_t reqId, int shard_idx,
                          const std::vector<Value> &values,
                          const std::vector<PreparedTransaction> &prepares);

    void ROCommitSlowCallback(uint64_t transaction_id, uint64_t reqId, int shard_idx,
                              uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit);

    void HandleWound(uint64_t transaction_id);

    // choose coordinator from participants
    void CalculateCoordinatorChoices();
    int ChooseCoordinator();

    // Choose nonblock time
    Timestamp ChooseNonBlockTimestamp();

    ViewFinder vf_;

    std::unordered_map<std::bitset<MAX_SHARDS>, int> coord_choices_;
    std::unordered_map<std::bitset<MAX_SHARDS>, uint16_t> min_lats_;

    Timestamp min_read_timestamp_;

    const strongstore::NetworkConfiguration &net_config_;
    const std::string client_region_;

    transport::Configuration &config_;

    State state_;

    // Unique ID for this client.
    uint64_t client_id_;

    // Ongoing transaction ID.
    uint64_t transaction_id_;

    // Ongoing transaction start time
    Timestamp start_time_;

    // Number of shards in SpanStore.
    uint64_t nshards_;

    // List of participants in the ongoing transaction.
    std::set<int> participants_;

    // Transport used by paxos client proxies.
    Transport *transport_;

    // Client for each shard.
    std::vector<ShardClient *> sclients_;

    // Partitioner
    Partitioner *part_;

    // TrueTime server.
    TrueTime &tt_;

    uint64_t lastReqId;
    std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

    Latency_t op_lat_;
    Latency_t commit_lat_;

    Consistency consistency_;

    bool debug_stats_;

    double nb_time_alpha_;
};

}  // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
