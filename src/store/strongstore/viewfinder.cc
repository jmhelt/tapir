#include "store/strongstore/viewfinder.h"

namespace strongstore {

ViewFinder::ViewFinder() : cur_transaction_id_{static_cast<uint64_t>(-1)} {}
ViewFinder::~ViewFinder() {}

void ViewFinder::StartRO(uint64_t transaction_id, const std::set<int> &participants) {
    participants_.clear();
    cur_transaction_id_ = transaction_id;
    participants_.insert(participants.begin(), participants.end());
}

void ViewFinder::CommitRO(uint64_t transaction_id) {
    return;
}

SnapshotResult ViewFinder::ReceiveFastPath(uint64_t transaction_id, int shard_idx) {
    ASSERT(participants_.count(shard_idx) > 0);
    participants_.erase(shard_idx);

    if (participants_.size() == 0) {  // Received all fast path responses
        return {COMMIT};
    } else {
        return {WAIT};
    }
}

SnapshotResult ViewFinder::ReceiveSlowPath() {
    return {};
}

};  // namespace strongstore