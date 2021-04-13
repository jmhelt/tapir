
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
        status_ = PREPARING;
    } else {
        status_ = WAIT_PARTICIPANTS;
    }
}

void TransactionStore::PendingRWTransaction::FinishCoordinatorPrepare(const Timestamp &prepare_ts) {
    prepare_ts_ = std::max(prepare_ts_, prepare_ts);
    commit_ts_ = prepare_ts_;
    status_ = PREPARED;
}

void TransactionStore::PendingROTransaction::StartRO(const std::unordered_set<std::string> &keys,
                                                     const Timestamp &min_ts,
                                                     const Timestamp &commit_ts) {
    keys_.insert(keys.begin(), keys.end());
    min_ts_ = min_ts;
    commit_ts_ = commit_ts;
    status_ = PREPARING;
}

void TransactionStore::StartGet(uint64_t transaction_id, const std::string &key) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READING);

    pt.AddReadSet(key);
}

void TransactionStore::FinishGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READING);
}

void TransactionStore::AbortGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READING ||
           pt.status() == READ_WAIT);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PauseGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READING);

    pt.set_status(READ_WAIT);
}

void TransactionStore::ContinueGet(uint64_t transaction_id, const std::string &key) {
    (void)key;
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READ_WAIT);

    pt.set_status(READING);
}

TransactionStatus TransactionStore::StartCoordinatorPrepare(uint64_t transaction_id, const Timestamp &start_ts,
                                                            int coordinator, const std::unordered_set<int> participants,
                                                            const Transaction &transaction,
                                                            const Timestamp &nonblock_ts) {
    if (aborted_.count(transaction_id) > 0) {
        return ABORTED;
    }

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == READING);

    Debug("[%lu] Coordinator: StartTransaction %lu.%lu", transaction_id, start_ts.getTimestamp(), start_ts.getID());

    pt.StartCoordinatorPrepare(start_ts, coordinator, participants, transaction, nonblock_ts);

    return pt.status();
}

void TransactionStore::FinishCoordinatorPrepare(uint64_t transaction_id, const Timestamp &prepare_ts) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == PREPARING);
    pt.FinishCoordinatorPrepare(prepare_ts);
}

TransactionStatus TransactionStore::GetTransactionStatus(uint64_t transaction_id) {
    auto search = pending_rw_.find(transaction_id);
    if (search == pending_rw_.end()) {
        return READING;
    } else {
        return search->second.status();
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
    ASSERT(pt.status() == PREPARED);
    return pt.commit_ts();
}

const Timestamp &TransactionStore::GetROCommitTimestamp(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.status() == PREPARED);
    return pt.commit_ts();
}

const std::unordered_set<std::string> &TransactionStore::GetROKeys(uint64_t transaction_id) {
    PendingROTransaction &pt = pending_ro_[transaction_id];
    ASSERT(pt.status() == PREPARED);
    return pt.keys();
}

void TransactionStore::AbortPrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == PREPARING ||
           pt.status() == PREPARE_WAIT ||
           pt.status() == WAIT_PARTICIPANTS);

    pending_rw_.erase(transaction_id);
    aborted_.insert(transaction_id);
}

void TransactionStore::PausePrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == PREPARING);

    pt.set_status(PREPARE_WAIT);
}
void TransactionStore::ContinuePrepare(uint64_t transaction_id) {
    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == PREPARE_WAIT);

    pt.set_status(PREPARING);
}

TransactionFinishResult TransactionStore::Commit(uint64_t transaction_id) {
    Debug("[%lu] COMMIT", transaction_id);

    TransactionFinishResult r;

    PendingRWTransaction &pt = pending_rw_[transaction_id];
    ASSERT(pt.status() == PREPARED);

    r.notify_ros = std::move(pt.waiting_ros());

    pending_rw_.erase(transaction_id);
    committed_.insert(transaction_id);

    return r;
}

uint64_t TransactionStore::StartRO(uint64_t transaction_id,
                                   const std::unordered_set<std::string> &keys,
                                   const Timestamp &min_ts,
                                   const Timestamp &commit_ts) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.status() == PREPARING);

    ro.StartRO(keys, min_ts, commit_ts);

    uint64_t n_conflicts = 0;

    ASSERT(min_ts < commit_ts);

    for (auto &p : pending_rw_) {
        PendingRWTransaction &rw = p.second;

        if (rw.status() != PREPARED) {
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

    if (n_conflicts == 0) {
        ro.set_status(PREPARED);
    } else {
        ro.set_status(PREPARE_WAIT);
    }

    stats_.IncrementList("n_conflicting_prepared", n_conflicts);
    return n_conflicts;
}

void TransactionStore::ContinueRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.status() == PREPARE_WAIT);

    ro.set_status(PREPARED);
}

void TransactionStore::CommitRO(uint64_t transaction_id) {
    PendingROTransaction &ro = pending_ro_[transaction_id];
    ASSERT(ro.status() == PREPARED);

    ro.set_status(COMMITTED);
    pending_ro_.erase(transaction_id);
    committed_.insert(transaction_id);
}

}  // namespace strongstore
