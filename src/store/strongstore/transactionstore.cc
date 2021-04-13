
#include "store/strongstore/transactionstore.h"

#include <algorithm>

namespace strongstore {

TransactionStore::TransactionStore(const TrueTime &tt, Consistency c) : tt_{tt}, consistency_{c} {}

TransactionStore::~TransactionStore() {}

void TransactionStore::PendingRWTransaction::AddReadSet(const std::string &key) {
    transaction_.addReadSet(key, Timestamp());
}

void TransactionStore::PendingRWTransaction::StartCoordinatorPrepare(const Timestamp &start_ts, int coordinator,
                                                                     const std::unordered_set<int> participants,
                                                                     const Transaction &transaction,
                                                                     const Timestamp &nonblock_ts) {
    ASSERT(participants.size() > 0);

    start_ts_ = start_ts;
    coordinator_ = coordinator;
    participants_ = participants;
    transaction_.add_read_write_sets(transaction);
    transaction_.set_start_time(transaction.start_time());
    nonblock_ts_ = nonblock_ts;
    commit_ts_ = std::max(commit_ts_, start_ts);

    if (participants_.size() == 1) {
        state_ = PREPARING;
    } else {
        state_ = WAIT_PARTICIPANTS;
    }
}

void TransactionStore::PendingRWTransaction::FinishCoordinatorPrepare(const Timestamp &prepare_ts) {
    prepare_ts_ = std::max(prepare_ts_, prepare_ts);
    commit_ts_ = prepare_ts_;
    state_ = COMMITTING;
}

void TransactionStore::PendingROTransaction::StartRO(const std::unordered_set<std::string> &keys,
                                                     const Timestamp &min_ts,
                                                     const Timestamp &commit_ts,
                                                     uint64_t n_conflicts) {
    keys_.insert(keys.begin(), keys.end());
    min_ts_ = min_ts;
    commit_ts_ = commit_ts;
    n_conflicts_ = n_conflicts;
    if (n_conflicts_ == 0) {
        state_ = COMMITTING;
    } else {
        state_ = PREPARE_WAIT;
    }
}

void TransactionStore::StartGet(uint64_t transaction_id, const std::string &key) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    pt.AddReadSet(key);
}

void TransactionStore::FinishGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);
}

void TransactionStore::AbortGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING ||
           pt.state() == READ_WAIT);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PauseGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    pt.set_state(READ_WAIT);
}

void TransactionStore::ContinueGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READ_WAIT);

    pt.set_state(READING);
}

TransactionState TransactionStore::StartCoordinatorPrepare(uint64_t transaction_id, const Timestamp &start_ts,
                                                           int coordinator, const std::unordered_set<int> participants,
                                                           const Transaction &transaction,
                                                           const Timestamp &nonblock_ts) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == READING);

    Debug("[%lu] Coordinator: StartTransaction %lu.%lu", transaction_id, start_ts.getTimestamp(), start_ts.getID());

    pt.StartCoordinatorPrepare(start_ts, coordinator, participants, transaction, nonblock_ts);

    return pt.state();
}

void TransactionStore::FinishCoordinatorPrepare(uint64_t transaction_id, const Timestamp &prepare_ts) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);
    pt.FinishCoordinatorPrepare(prepare_ts);
}

TransactionState TransactionStore::GetTransactionState(uint64_t transaction_id) {
    if (committed_.count(transaction_id) > 0) {
        return COMMITTED;
    }

    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    auto search = pending_rw_.find(transaction_id);
    if (search == pending_rw_.end()) {
        return NOT_FOUND;
    } else {
        return search->second.state();
    }
}

const Timestamp &TransactionStore::GetStartTimestamp(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.start_ts();
}

const std::unordered_set<int> &TransactionStore::GetParticipants(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.participants();
}

const Timestamp &TransactionStore::GetNonBlockTimestamp(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.nonblock_ts();
}

const Transaction &TransactionStore::GetTransaction(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    ASSERT(search != pending_rw_.end());
    return search->second.transaction();
}

const Timestamp &TransactionStore::GetRWCommitTimestamp(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.commit_ts();
}

const Timestamp &TransactionStore::GetROCommitTimestamp(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.commit_ts();
}

const std::unordered_set<std::string> &TransactionStore::GetROKeys(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.state() == COMMITTING);
    return pt.keys();
}

void TransactionStore::AbortPrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING ||
           pt.state() == PREPARE_WAIT ||
           pt.state() == WAIT_PARTICIPANTS);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PausePrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARING);

    pt.set_state(PREPARE_WAIT);
}
void TransactionStore::ContinuePrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == PREPARE_WAIT);

    pt.set_state(PREPARING);
}

void TransactionStore::NotifyROs(std::unordered_set<uint64_t> &ros) {
    for (auto it = ros.begin(); it != ros.end();) {
        ASSERT(pending_ro_.find(*it) != pending_ro_.end());
        PendingROTransaction &pt = pending_ro_[*it];
        pt.decr_conflicts();

        if (pt.n_conflicts() > 0) {
            it = ros.erase(it);
        } else {
            it++;
        }
    }
}

TransactionFinishResult TransactionStore::Commit(uint64_t transaction_id) {
    Debug("[%lu] COMMIT", transaction_id);

    TransactionFinishResult r;

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() == COMMITTING);

    r.notify_ros = std::move(pt.waiting_ros());
    NotifyROs(r.notify_ros);

    pending_rw_.erase(transaction_id);
    committed_.insert(transaction_id);

    return r;
}

TransactionFinishResult TransactionStore::Abort(uint64_t transaction_id) {
    Debug("[%lu] ABORT", transaction_id);

    TransactionFinishResult r;

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.state() != COMMITTING &&
           pt.state() != COMMITTED &&
           pt.state() != ABORTED);

    r.notify_ros = std::move(pt.waiting_ros());
    NotifyROs(r.notify_ros);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);

    return r;
}

TransactionState TransactionStore::StartRO(uint64_t transaction_id,
                                           const std::unordered_set<std::string> &keys,
                                           const Timestamp &min_ts,
                                           const Timestamp &commit_ts) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == PREPARING);

    uint64_t n_conflicts = 0;

    ASSERT(min_ts < commit_ts);

    for (auto &p : pending_rw_) {
        PendingRWTransaction &rw = p.second;

        if (rw.state() != PREPARED && rw.state() != COMMITTING) {
            continue;
        }

        if (commit_ts < rw.prepare_ts()) {
            Debug("[%lu] Not waiting for prepared transaction (prepare): %lu < %lu",
                  transaction_id, commit_ts.getTimestamp(),
                  rw.prepare_ts().getTimestamp());
            continue;
        }

        if (consistency_ == Consistency::RSS &&
            min_ts < rw.prepare_ts() && commit_ts < rw.nonblock_ts()) {
            Debug("[%lu] Not waiting for prepared transaction (nonblock): %lu < %lu",
                  transaction_id, commit_ts.getTimestamp(),
                  rw.nonblock_ts().getTimestamp());
            continue;
        } else {
            Debug("min: %lu >= %lu", min_ts.getTimestamp(), rw.prepare_ts().getTimestamp());

            Debug("nb: %lu >= %lu", commit_ts.getTimestamp(), rw.nonblock_ts().getTimestamp());
        }

        const Transaction &transaction = rw.transaction();

        for (auto &w : transaction.getWriteSet()) {
            if (keys.count(w.first) != 0) {
                Debug("%lu conflicts with %lu", transaction_id, p.first);
                rw.add_waiting_ro(transaction_id);
                n_conflicts += 1;
                break;
            }
        }
    }

    ro.StartRO(keys, min_ts, commit_ts, n_conflicts);

    stats_.IncrementList("n_conflicting_prepared", n_conflicts);
    return ro.state();
}

void TransactionStore::ContinueRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == PREPARE_WAIT);

    ro.set_state(COMMITTING);
}

void TransactionStore::CommitRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.state() == COMMITTING);

    ro.set_state(COMMITTED);
    pending_ro_.erase(transaction_id);
    committed_.insert(transaction_id);
}

}  // namespace strongstore
