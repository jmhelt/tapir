#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <bitset>
#include <memory>
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

class ContextState {
   public:
    ContextState() : participants_{}, state_{EXECUTING} {}
    ~ContextState() {}

    bool executing() const { return (state_ == EXECUTING); }
    bool committing() const { return (state_ == COMMITTING); }
    bool aborted() const { return (state_ == ABORTED); }

   protected:
    friend class Client;

    void set_committing() { state_ = COMMITTING; }
    void set_aborted() { state_ = ABORTED; }

    const std::set<int> &participants() const { return participants_; }
    void add_participant(int p) { participants_.insert(p); }
    void clear_participants() { participants_.clear(); }

   private:
    enum State {
        EXECUTING,
        COMMITTING,
        ABORTED
    };

    std::set<int> participants_;
    State state_;
};

class Client : public ::Client {
   public:
    Client(Consistency consistency, const NetworkConfiguration &net_config,
           const std::string &client_region, transport::Configuration &config,
           uint64_t id, int nshards, int closestReplic, Transport *transport,
           Partitioner *part, TrueTime &tt, bool debug_stats,
           double nb_time_alpha);
    virtual ~Client();

    // Overriding functions from ::Client
    // Begin a transaction
    virtual void Begin(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;

    // Begin a retried transaction.
    virtual void Begin(Context &ctx, begin_callback bcb,
                       begin_timeout_callback btcb, uint32_t timeout) override;

    // Get the value corresponding to key.
    virtual void Get(Context &ctx, const std::string &key,
                     get_callback gcb, get_timeout_callback gtcb,
                     uint32_t timeout = GET_TIMEOUT) override;

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    virtual void GetForUpdate(Context &ctx, const std::string &key,
                              get_callback gcb, get_timeout_callback gtcb,
                              uint32_t timeout = GET_TIMEOUT) override;

    // Set the value for the given key.
    virtual void Put(Context &ctx, const std::string &key, const std::string &value,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout = PUT_TIMEOUT) override;

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(Context &ctx, commit_callback ccb, commit_timeout_callback ctcb,
                        uint32_t timeout) override;

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(Context &ctx, abort_callback acb, abort_timeout_callback atcb,
                       uint32_t timeout) override;

    // Commit all Get(s) and Put(s) since Begin().
    void ROCommit(Context &ctx, const std::unordered_set<std::string> &keys,
                  commit_callback ccb, commit_timeout_callback ctcb,
                  uint32_t timeout) override;

   private:
    const static std::size_t MAX_SHARDS = 16;

    struct PendingRequest {
        PendingRequest(uint64_t id)
            : id(id), outstandingPrepares(0) {}

        ~PendingRequest() {}

        commit_callback ccb;
        commit_timeout_callback ctcb;
        abort_callback acb;
        abort_timeout_callback atcb;
        uint64_t id;
        int outstandingPrepares;
    };

    void Abort(const uint64_t transaction_id, abort_callback acb, abort_timeout_callback atcb,
               uint32_t timeout);

    // local Prepare function
    void CommitCallback(Context &ctx, uint64_t req_id, int status, Timestamp commit_ts, Timestamp nonblock_ts);

    void AbortCallback(const uint64_t transaction_id, uint64_t req_id);

    void ROCommitCallback(Context &ctx, uint64_t req_id, int shard_idx,
                          const std::vector<Value> &values,
                          const std::vector<PreparedTransaction> &prepares);

    void ROCommitSlowCallback(Context &ctx, uint64_t req_id, int shard_idx,
                              uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit);

    void HandleWound(const uint64_t transaction_id);

    // choose coordinator from participants
    void CalculateCoordinatorChoices();
    int ChooseCoordinator(const uint64_t transaction_id);

    // Choose nonblock time
    Timestamp ChooseNonBlockTimestamp(const uint64_t transaction_id);

    ViewFinder vf_;

    std::unordered_map<std::bitset<MAX_SHARDS>, int> coord_choices_;
    std::unordered_map<std::bitset<MAX_SHARDS>, uint16_t> min_lats_;

    std::unordered_map<uint64_t, std::unique_ptr<ContextState>> context_states_;

    const strongstore::NetworkConfiguration &net_config_;
    const std::string client_region_;

    transport::Configuration &config_;

    // Unique ID for this client.
    uint64_t client_id_;

    // Number of shards in SpanStore.
    uint64_t nshards_;

    // Transport used by paxos client proxies.
    Transport *transport_;

    // Client for each shard.
    std::vector<ShardClient *> sclients_;

    // Partitioner
    Partitioner *part_;

    // TrueTime server.
    TrueTime &tt_;

    uint64_t next_transaction_id_;

    uint64_t last_req_id_;
    std::unordered_map<uint64_t, PendingRequest *> pending_reqs_;

    Latency_t op_lat_;
    Latency_t commit_lat_;

    Consistency consistency_;

    double nb_time_alpha_;

    bool debug_stats_;
};

}  // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
