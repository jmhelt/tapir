#include "store/strongstore/viewfinder.h"

namespace strongstore {

ViewFinder::ViewFinder() : cur_transaction_id_{static_cast<uint64_t>(-1)} {}
ViewFinder::~ViewFinder() {}

void ViewFinder::StartRO(uint64_t transaction_id, const std::set<int> &participants) {
    values_.clear();
    prepares_.clear();
    participants_.clear();
    cur_transaction_id_ = transaction_id;
    participants_.insert(participants.begin(), participants.end());
}

void ViewFinder::CommitRO(uint64_t transaction_id) {
    return;
}

SnapshotResult ViewFinder::ReceiveFastPath(uint64_t transaction_id, int shard_idx,
                                           const std::vector<Value> &values,
                                           const std::vector<PreparedTransaction> &prepares) {
    Debug("[%lu] Received fast path RO response", transaction_id);

    ASSERT(participants_.count(shard_idx) > 0);
    participants_.erase(shard_idx);

    values_.insert(values_.end(), values.begin(), values.end());

    for (auto &p : prepares) {
        auto search = prepares_.find(p.transaction_id());
        if (search == prepares_.end()) {
            prepares_.insert(search, {p.transaction_id(), p});
        } else {
            PreparedTransaction &pt = search->second;
            ASSERT(pt.transaction_id() == p.transaction_id());
            pt.update_prepare_ts(p.prepare_ts());
            pt.add_write_set(p.write_set());
        }
    }

    if (participants_.size() == 0) {  // Received all fast path responses
        FindCommittedKeys();
        CheckCommit();
        return {COMMIT};
    } else {
        return {WAIT};
    }
}

void ViewFinder::FindCommittedKeys() {
    for (auto &v : values_) {
        Debug("value: %lu %lu.%lu %s %s", v.transaction_id(), v.ts().getTimestamp(), v.ts().getID(), v.key().c_str(), v.val().c_str());
    }

    for (auto &p : prepares_) {
        Debug("prepare: %lu %lu.%lu", p.second.transaction_id(), p.second.prepare_ts().getTimestamp(), p.second.prepare_ts().getID());
    }

    if (prepares_.size() == 0) {
        return;
    }

    for (Value &v : values_) {
        uint64_t transaction_id = v.transaction_id();
        auto search = prepares_.find(transaction_id);
        if (search != prepares_.end()) {
            PreparedTransaction &pt = search->second;

            for (auto &write : pt.write_set()) {
                values_.emplace_back(transaction_id, v.ts(), write.first, write.second);
            }

            prepares_.erase(search);
        }
    }
}

void ViewFinder::CheckCommit() {
    // Find min prepare ts
    Timestamp min_ts = Timestamp::MAX;
    for (auto &p : prepares_) {
        PreparedTransaction &pt = p.second;
        if (pt.prepare_ts() < min_ts) {
            min_ts = pt.prepare_ts();
        }
    }

    // Find max commit ts
    Timestamp max_ts{0, 0};
    for (Value &v : values_) {
        if (max_ts < v.ts()) {
            max_ts = v.ts();
        }
    }

    Debug("min prepare ts: %lu.%lu", min_ts.getTimestamp(), min_ts.getID());
    Debug("max commit ts: %lu.%lu", max_ts.getTimestamp(), max_ts.getID());
    Debug("can commit: %d", max_ts < min_ts);
}

SnapshotResult ViewFinder::ReceiveSlowPath() {
    return {};
}
};  // namespace strongstore