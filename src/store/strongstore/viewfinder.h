#ifndef _STRONG_VIEW_FINDER_H_
#define _STRONG_VIEW_FINDER_H_

#include <cstdint>
#include <list>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
#include "store/strongstore/common.h"
#include "store/strongstore/preparedtransaction.h"

namespace strongstore {

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

class ViewFinder {
   public:
    ViewFinder(Consistency consistency);
    ~ViewFinder();

    void StartRO(uint64_t transaction_id, const std::set<int> &participants);
    void CommitRO(uint64_t transaction_id);

    SnapshotResult ReceiveFastPath(uint64_t transaction_id, int shard_idx,
                                   const std::vector<Value> &values,
                                   const std::vector<PreparedTransaction> &prepares);

    SnapshotResult ReceiveSlowPath(uint64_t transaction_id, uint64_t rw_transaction_id,
                                   bool is_commit, const Timestamp &commit_ts);

    SnapshotResult ReceiveSlowPath();

    SnapshotResult FindSnapshot(std::unordered_map<uint64_t, PreparedTransaction> &prepared,
                                std::vector<CommittedTransaction> &committed);

   private:
    uint64_t cur_transaction_id_;
    Timestamp snapshot_ts_;
    std::unordered_set<int> participants_;
    std::unordered_map<std::string, std::list<Value>> values_;
    std::unordered_map<uint64_t, PreparedTransaction> prepares_;
    Consistency consistency_;

    void AddValues(const std::vector<Value> &values);
    void AddPrepares(const std::vector<PreparedTransaction> &prepares);

    void ReceivedAllFastPaths();
    void FindCommittedKeys();
    void CalculateSnapshotTimestamp();
    SnapshotResult CheckCommit();
};

}  // namespace strongstore

#endif /* _STRONG_VIEW_FINDER_H_ */
