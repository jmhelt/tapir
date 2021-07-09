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
#include "store/strongstore/preparedtransaction.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class ContextState {
   public:
    ContextState()
        : participants_{}, prepares_{}, values_{}, snapshot_ts_{}, current_participant_{-1}, state_{EXECUTING} {}
    ~ContextState() {}

    const std::set<int> &participants() const { return participants_; }
    const std::unordered_map<uint64_t, PreparedTransaction> prepares() const { return prepares_; }

    const Timestamp &snapshot_ts() const { return snapshot_ts_; }

   protected:
    friend class Client;

    enum State {
        EXECUTING = 0,
        GETTING,
        PUTTING,
        COMMITTING,
        NEEDS_ABORT,
        ABORTING
    };

    State state() const { return state_; }

    bool executing() const { return (state_ == EXECUTING); }
    bool needs_aborts() const { return (state_ == NEEDS_ABORT); }

    int current_participant() const { return current_participant_; }

    void set_executing() {
        current_participant_ = -1;
        state_ = EXECUTING;
    }

    void set_getting(int p) {
        current_participant_ = p;
        state_ = GETTING;
    }

    void set_putting(int p) {
        current_participant_ = p;
        state_ = PUTTING;
    }

    void set_committing() { state_ = COMMITTING; }
    void set_needs_abort() { state_ = NEEDS_ABORT; }
    void set_aborting() { state_ = ABORTING; }

    std::set<int> &mutable_participants() { return participants_; }
    void add_participant(int p) { participants_.insert(p); }
    void clear_participants() { participants_.clear(); }

    std::unordered_map<uint64_t, PreparedTransaction> &mutable_prepares() { return prepares_; }
    std::unordered_map<std::string, std::list<Value>> &mutable_values() { return values_; }

    void set_snapshot_ts(const Timestamp &ts) { snapshot_ts_ = ts; }

   private:
    std::set<int> participants_;
    std::unordered_map<uint64_t, PreparedTransaction> prepares_;
    std::unordered_map<std::string, std::list<Value>> values_;
    Timestamp snapshot_ts_;
    int current_participant_;
    State state_;
};

class CommittedTransaction {
   public:
    uint64_t transaction_id;
    Timestamp commit_ts;
    bool committed;
};

enum SnapshotState {
    WAIT,
    COMMIT
};

struct SnapshotResult {
    SnapshotState state;
    Timestamp max_read_ts;
    std::unordered_map<std::string, std::string> kv_;
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
    // Begin a RW transaction
    virtual void BeginRW(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;
    virtual void BeginRW(std::unique_ptr<Context> &ctx, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;

    // Begin a RO transaction
    virtual void BeginRO(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;
    virtual void BeginRO(std::unique_ptr<Context> &ctx, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;

    // Begin a retried transaction.
    virtual void Retry(std::unique_ptr<Context> &ctx, begin_callback bcb,
                       begin_timeout_callback btcb, uint32_t timeout) override;

    // Get the value corresponding to key.
    virtual void Get(std::unique_ptr<Context> &ctx, const std::string &key,
                     get_callback gcb, get_timeout_callback gtcb,
                     uint32_t timeout = GET_TIMEOUT) override;

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    virtual void GetForUpdate(std::unique_ptr<Context> &ctx, const std::string &key,
                              get_callback gcb, get_timeout_callback gtcb,
                              uint32_t timeout = GET_TIMEOUT) override;

    // Set the value for the given key.
    virtual void Put(std::unique_ptr<Context> &ctx, const std::string &key, const std::string &value,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout = PUT_TIMEOUT) override;

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(std::unique_ptr<Context> &ctx, commit_callback ccb, commit_timeout_callback ctcb,
                        uint32_t timeout) override;

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(std::unique_ptr<Context> &ctx, abort_callback acb, abort_timeout_callback atcb,
                       uint32_t timeout) override;

    // Commit all Get(s) and Put(s) since Begin().
    void ROCommit(std::unique_ptr<Context> &ctx, const std::unordered_set<std::string> &keys,
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

    std::unique_ptr<Context> Begin();
    std::unique_ptr<Context> Begin(std::unique_ptr<Context> &ctx);

    // local Prepare function
    void CommitCallback(std::unique_ptr<Context> &ctx, uint64_t req_id, int status, Timestamp commit_ts, Timestamp nonblock_ts);

    void AbortCallback(std::unique_ptr<Context> &ctx, uint64_t req_id);

    void ROCommitCallback(std::unique_ptr<Context> &ctx, uint64_t req_id, int shard_idx,
                          const std::vector<Value> &values,
                          const std::vector<PreparedTransaction> &prepares);

    void ROCommitSlowCallback(std::unique_ptr<Context> &ctx, uint64_t req_id, int shard_idx,
                              uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit);

    void HandleWound(const uint64_t transaction_id);

    // choose coordinator from participants
    void CalculateCoordinatorChoices();
    int ChooseCoordinator(const uint64_t transaction_id);

    // Choose nonblock time
    Timestamp ChooseNonBlockTimestamp(const uint64_t transaction_id);

    // For tracking RO reply progress
    SnapshotResult ReceiveFastPath(uint64_t transaction_id, std::unique_ptr<ContextState> &state,
                                   int shard_idx,
                                   const std::vector<Value> &values,
                                   const std::vector<PreparedTransaction> &prepares);
    SnapshotResult ReceiveSlowPath(uint64_t transaction_id, std::unique_ptr<ContextState> &state,
                                   uint64_t rw_transaction_id,
                                   bool is_commit, const Timestamp &commit_ts);
    SnapshotResult FindSnapshot(std::unordered_map<uint64_t, PreparedTransaction> &prepared,
                                std::vector<CommittedTransaction> &committed);
    void AddValues(std::unique_ptr<ContextState> &state, const std::vector<Value> &values);
    void AddPrepares(std::unique_ptr<ContextState> &state, const std::vector<PreparedTransaction> &prepares);
    void ReceivedAllFastPaths(std::unique_ptr<ContextState> &state);
    void FindCommittedKeys(std::unique_ptr<ContextState> &state);
    void CalculateSnapshotTimestamp(std::unique_ptr<ContextState> &state);
    SnapshotResult CheckCommit(std::unique_ptr<ContextState> &state);

    std::unordered_map<std::bitset<MAX_SHARDS>, int> coord_choices_;
    std::unordered_map<std::bitset<MAX_SHARDS>, uint16_t> min_lats_;

    std::unordered_map<uint64_t, std::unique_ptr<ContextState>> context_states_;

    const strongstore::NetworkConfiguration &net_config_;
    const std::string client_region_;

    const std::string service_name_;

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
