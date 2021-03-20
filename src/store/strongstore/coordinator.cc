
#include "store/strongstore/coordinator.h"

namespace strongstore {

Coordinator::Coordinator(const TrueTime &tt)
    : tt_{tt}, prepared_transactions_{}, aborted_transactions_{} {}

Coordinator::~Coordinator() {}

Transaction Coordinator::GetTransaction(uint64_t transaction_id) {
    auto search = prepared_transactions_.find(transaction_id);
    ASSERT(search != prepared_transactions_.end());
    return search->second.GetTransaction();
}

int Coordinator::GetNParticipants(uint64_t transaction_id) {
    auto search = prepared_transactions_.find(transaction_id);
    ASSERT(search != prepared_transactions_.end());
    return search->second.GetNParticipants();
}

Decision Coordinator::StartTransaction(uint64_t client_id,
                                       uint64_t transaction_id,
                                       int n_participants,
                                       Transaction transaction) {
    auto now = tt_.Now();
    Timestamp start_timestamp{now.latest(), client_id};
    Debug("Coordinator: StartTransaction %lu %lu.%lu %d", transaction_id,
          start_timestamp.getTimestamp(), start_timestamp.getID(),
          n_participants);
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return Decision::ABORT;
    }

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.StartTransaction(start_timestamp, n_participants, transaction);

    bool try_coord = pt.TryCoord();

    // prepared_transactions_[transaction_id] = std::move(pt);

    if (try_coord) {
        return Decision::TRY_COORD;
    } else {
        return Decision::WAIT;
    }
}

CommitDecision Coordinator::ReceivePrepareOK(uint64_t transaction_id,
                                             int shard_idx,
                                             Timestamp &prepare_timestamp) {
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return {Decision::ABORT, {}};
    }

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.PrepareOK(shard_idx, prepare_timestamp);

    bool try_coord = pt.TryCoord();
    bool can_commit = pt.CanCommit();
    Timestamp commit_timestamp = pt.GetTimeCommit();

    // prepared_transactions_[transaction_id] = std::move(pt);

    if (can_commit) {
        return {commit_timestamp, Decision::COMMIT};
    } else if (try_coord) {
        return {Decision::TRY_COORD};
    } else {
        return {Decision::WAIT};
    }
}

void Coordinator::Commit(uint64_t transaction_id) {
    prepared_transactions_.erase(transaction_id);
}

void Coordinator::Abort(uint64_t transaction_id) {
    aborted_transactions_.insert(transaction_id);
    prepared_transactions_.erase(transaction_id);
}

uint64_t Coordinator::CommitWaitMs(Timestamp &commit_timestamp) {
    uint64_t timestamp = commit_timestamp.getTimestamp();
    auto now = tt_.Now();
    Debug("CommitWaitMs: %lu ? %lu", timestamp, now.earliest());
    if (timestamp <= now.earliest()) {
        return 0;
    } else {
        return timestamp - now.earliest() * 1000;
    }
}
};  // namespace strongstore