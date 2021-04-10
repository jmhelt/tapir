
#include "store/strongstore/coordinator.h"

namespace strongstore {

Coordinator::Coordinator(const TrueTime &tt)
    : tt_{tt}, prepared_transactions_{}, aborted_transactions_{} {}

Coordinator::~Coordinator() {}

bool Coordinator::HasTransaction(uint64_t transaction_id) {
    return prepared_transactions_.find(transaction_id) !=
           prepared_transactions_.end();
}

Transaction &Coordinator::GetTransaction(uint64_t transaction_id) {
    auto search = prepared_transactions_.find(transaction_id);
    ASSERT(search != prepared_transactions_.end());
    return search->second.transaction();
}

Decision Coordinator::StartTransaction(uint64_t client_id,
                                       uint64_t transaction_id,
                                       uint64_t n_participants,
                                       Transaction transaction) {
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return Decision::ABORT;
    }

    auto now = tt_.Now();
    Timestamp start_timestamp{now.latest(), client_id};
    Debug("Coordinator: StartTransaction %lu %lu.%lu %lu", transaction_id,
          start_timestamp.getTimestamp(), start_timestamp.getID(),
          n_participants);

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.StartTransaction(start_timestamp, n_participants, transaction);

    if (pt.TryCoord()) {
        return Decision::TRY_COORD;
    } else {
        return Decision::WAIT;
    }
}

CommitDecision Coordinator::ReceivePrepareOK(
    uint64_t transaction_id, int shard_idx,
    const Timestamp &prepare_timestamp) {
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return {Timestamp(), Decision::ABORT};
    }

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.PrepareOK(shard_idx, prepare_timestamp);

    if (pt.CanCommit()) {
        return {pt.GetTimeCommit(), Decision::COMMIT};
    } else if (pt.TryCoord()) {
        return {Timestamp(), Decision::TRY_COORD};
    } else {
        return {Timestamp(), Decision::WAIT};
    }
}

void Coordinator::Commit(uint64_t transaction_id) {
    prepared_transactions_.erase(transaction_id);
}

void Coordinator::Abort(uint64_t transaction_id) {
    aborted_transactions_.insert(transaction_id);
    prepared_transactions_.erase(transaction_id);
}

uint64_t Coordinator::CommitWaitMS(const Timestamp &commit_timestamp) const {
    return tt_.TimeToWaitUntilMS(commit_timestamp.getTimestamp());
}
};  // namespace strongstore