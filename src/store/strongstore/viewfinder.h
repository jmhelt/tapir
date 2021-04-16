#ifndef _STRONG_VIEW_FINDER_H_
#define _STRONG_VIEW_FINDER_H_

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
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
    ViewFinder();
    ~ViewFinder();

    void StartRO(uint64_t transaction_id, const std::set<int> &participants);
    void CommitRO(uint64_t transaction_id);

    SnapshotResult ReceiveFastPath(uint64_t transaction_id, int shard_idx,
                                   const std::vector<Value> &values,
                                   const std::vector<PreparedTransaction> &prepares);

    SnapshotResult ReceiveSlowPath();

    SnapshotResult FindSnapshot(std::unordered_map<uint64_t, PreparedTransaction> &prepared,
                                std::vector<CommittedTransaction> &committed);

   private:
    uint64_t cur_transaction_id_;
    std::unordered_set<int> participants_;
    std::vector<Value> values_;
    std::unordered_map<uint64_t, PreparedTransaction> prepares_;

    void FindCommittedKeys();
};

}  // namespace strongstore

#endif /* _STRONG_VIEW_FINDER_H_ */
