#include "store/strongstore/viewfinder.h"

namespace strongstore {

ViewFinder::ViewFinder(Consistency consistency)
    : cur_transaction_id_{static_cast<uint64_t>(-1)}, consistency_{consistency} {}

ViewFinder::~ViewFinder() {}

void ViewFinder::StartRO(uint64_t transaction_id, const std::set<int> &participants) {
    values_.clear();
    prepares_.clear();
    participants_.clear();
    cur_transaction_id_ = transaction_id;
    participants_.insert(participants.begin(), participants.end());
}

void ViewFinder::CommitRO(uint64_t transaction_id) {
    cur_transaction_id_ = static_cast<uint64_t>(-1);
    return;
}

SnapshotResult ViewFinder::ReceiveFastPath(uint64_t transaction_id, int shard_idx,
                                           const std::vector<Value> &values,
                                           const std::vector<PreparedTransaction> &prepares) {
    Debug("[%lu] Received fast path RO response", transaction_id);

    ASSERT(participants_.count(shard_idx) > 0);
    participants_.erase(shard_idx);

    AddValues(values);
    AddPrepares(prepares);

    if (participants_.size() == 0) {  // Received all fast path responses
        ReceivedAllFastPaths();

        if (consistency_ == SS) {
            return {COMMIT, snapshot_ts_};
        } else if (consistency_ == RSS) {
            return CheckCommit();
        } else {
            NOT_REACHABLE();
        }
    } else {
        return {WAIT};
    }
}

SnapshotResult ViewFinder::ReceiveSlowPath(uint64_t transaction_id, uint64_t rw_transaction_id,
                                           bool is_commit, const Timestamp &commit_ts) {
    Debug("[%lu] Received fast path RO response", transaction_id);
    ASSERT(transaction_id != cur_transaction_id_);
    ASSERT(consistency_ == RSS);

    auto search = prepares_.find(transaction_id);
    if (search == prepares_.end()) {
        Debug("[%lu] already received commit decision for %lu", transaction_id, rw_transaction_id);
        return {WAIT};
    }

    if (is_commit) {
        PreparedTransaction &pt = search->second;
        Debug("[%lu] adding writes from prepared transaction: %lu", transaction_id, rw_transaction_id);

        std::vector<Value> values;
        for (auto &write : pt.write_set()) {
            values.emplace_back(transaction_id, commit_ts, write.first, write.second);
        }

        AddValues(values);
    }

    prepares_.erase(search);

    return CheckCommit();
}

void ViewFinder::AddValues(const std::vector<Value> &values) {
    for (auto &v : values) {
        std::list<Value> &l = values_[v.key()];

        std::list<Value>::iterator it = l.begin();
        for (; it != l.end(); ++it) {
            Value &v2 = *it;
            if (v2.ts() < v.ts()) {
                break;
            }
        }

        l.insert(it, v);
    }
}

void ViewFinder::AddPrepares(const std::vector<PreparedTransaction> &prepares) {
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
}

void ViewFinder::ReceivedAllFastPaths() {
    FindCommittedKeys();
    CalculateSnapshotTimestamp();
}

void ViewFinder::FindCommittedKeys() {
    if (prepares_.size() == 0) {
        return;
    }

    ASSERT(consistency_ == RSS);

    std::vector<Value> to_add;
    for (auto &kv : values_) {
        std::list<Value> &l = kv.second;
        for (Value &v : l) {
            uint64_t transaction_id = v.transaction_id();
            auto search = prepares_.find(transaction_id);
            if (search != prepares_.end()) {
                PreparedTransaction &pt = search->second;
                Debug("adding writes from prepared transaction: %lu", transaction_id);

                for (auto &write : pt.write_set()) {
                    to_add.emplace_back(transaction_id, v.ts(), write.first, write.second);
                }

                prepares_.erase(search);
            }
        }
    }

    AddValues(to_add);
}

void ViewFinder::CalculateSnapshotTimestamp() {
    // Find snapshot ts
    Timestamp snapshot_ts{0, 0};
    for (auto &kv : values_) {
        const std::list<Value> &l = kv.second;
        for (const Value &v : l) {
            if (snapshot_ts < v.ts()) {
                snapshot_ts = v.ts();
            }
        }
    }

    snapshot_ts_ = snapshot_ts;
}

SnapshotResult ViewFinder::CheckCommit() {
    for (auto &kv : values_) {
        Debug("key: %s", kv.first.c_str());
        std::list<Value> &l = kv.second;
        for (Value &v : l) {
            Debug("value: %lu %lu.%lu %s", v.transaction_id(), v.ts().getTimestamp(), v.ts().getID(), v.val().c_str());
        }
    }

    for (auto &p : prepares_) {
        Debug("prepare: %lu %lu.%lu", p.second.transaction_id(), p.second.prepare_ts().getTimestamp(), p.second.prepare_ts().getID());
        for (auto &write : p.second.write_set()) {
            Debug("write: %s %s", write.first.c_str(), write.second.c_str());
        }
    }

    // Find min prepare ts
    Timestamp min_ts = Timestamp::MAX;
    for (auto &p : prepares_) {
        PreparedTransaction &pt = p.second;
        if (pt.prepare_ts() < min_ts) {
            min_ts = pt.prepare_ts();
        }
    }

    Debug("min prepare ts: %lu.%lu", min_ts.getTimestamp(), min_ts.getID());
    Debug("snapshot ts: %lu.%lu", snapshot_ts_.getTimestamp(), snapshot_ts_.getID());
    Debug("can commit: %d", snapshot_ts_ < min_ts);

    if (snapshot_ts_ < min_ts) {
        return {COMMIT, snapshot_ts_};
    } else {
        return {WAIT};
    }
}

SnapshotResult ViewFinder::ReceiveSlowPath() {
    return {};
}
};  // namespace strongstore