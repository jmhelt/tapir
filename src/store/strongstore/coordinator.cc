
#include "store/strongstore/coordinator.h"

namespace strongstore {

Coordinator::Coordinator(TrueTime &tt)
    : tt{tt}, prepared_transactions_{}, aborted_transactions_{} {}

Coordinator::~Coordinator() {}

std::unordered_set<replication::RequestID> Coordinator::GetRequestIDs(
    uint64_t transaction_id) {
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return {};
    }

    auto search = prepared_transactions_.find(transaction_id);
    ASSERT(search != prepared_transactions_.end());
    return search->second.GetRequestIDs();
}

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

Decision Coordinator::StartTransaction(replication::RequestID rid,
                                       uint64_t transaction_id,
                                       int n_participants,
                                       Transaction transaction) {
    auto now = tt.Now();
    uint64_t start_timestamp = now.latest();
    Debug("Coordinator: StartTransaction %lu %lu %lu %lu %d", rid.client_id,
          rid.request_id, transaction_id, start_timestamp, n_participants);
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return Decision::ABORT;
    }

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.StartTransaction(rid, start_timestamp, n_participants, transaction);

    bool try_coord = pt.TryCoord();

    prepared_transactions_[transaction_id] = std::move(pt);

    if (try_coord) {
        return Decision::TRY_COORD;
    } else {
        return Decision::WAIT;
    }
}

CommitDecision Coordinator::ReceivePrepareOK(replication::RequestID rid,
                                             uint64_t transaction_id,
                                             int shardID,
                                             uint64_t timePrepare) {
    if (aborted_transactions_.find(transaction_id) !=
        aborted_transactions_.end()) {
        return {Decision::ABORT, 0};
    }

    PreparedTransaction &pt = prepared_transactions_[transaction_id];
    pt.PrepareOK(rid, shardID, timePrepare);

    bool try_coord = pt.TryCoord();
    bool can_commit = pt.CanCommit();
    uint64_t commit_time = pt.GetTimeCommit();

    prepared_transactions_[transaction_id] = std::move(pt);

    if (can_commit) {
        return {Decision::COMMIT, commit_time};
    } else if (try_coord) {
        return {Decision::TRY_COORD, 0};
    } else {
        return {Decision::WAIT, 0};
    }
}

void Coordinator::Commit(uint64_t transaction_id) {
    prepared_transactions_.erase(transaction_id);
}

void Coordinator::Abort(uint64_t transaction_id) {
    aborted_transactions_.insert(transaction_id);
    prepared_transactions_.erase(transaction_id);
}

uint64_t Coordinator::CommitWaitMs(uint64_t commit_timestamp) {
    auto now = tt.Now();
    Debug("CommitWaitMs: %lu ? %lu", commit_timestamp, now.earliest());
    if (commit_timestamp <= now.earliest()) {
        return 0;
    } else {
        return commit_timestamp - now.earliest() * 1000;
    }
}
};  // namespace strongstore