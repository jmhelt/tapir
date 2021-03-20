// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _STRONG_COORDINATOR_H_
#define _STRONG_COORDINATOR_H_

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/message.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"

namespace strongstore {

enum Decision { WAIT = 1, TRY_COORD = 2, COMMIT = 3, ABORT = 4 };

struct CommitDecision {
    Timestamp commit_timestamp;
    Decision d;
};

class PreparedTransaction {
   public:
    PreparedTransaction()
        : commit_timestamp_{},
          n_participants_{-1},
          ok_participants_{},
          transaction_{} {}
    PreparedTransaction(int shard_idx)
        : commit_timestamp_{},
          n_participants_{-1},
          ok_participants_{{shard_idx}},
          transaction_{} {}
    PreparedTransaction(Timestamp &start_timestamp, int n_participants,
                        Transaction transaction)
        : commit_timestamp_{start_timestamp},
          n_participants_{n_participants},
          ok_participants_{},
          transaction_{transaction} {}

    ~PreparedTransaction() {}

    int GetNParticipants() { return n_participants_; }

    Transaction GetTransaction() { return transaction_; }

    void StartTransaction(Timestamp &start_timestamp, int n_participants,
                          Transaction transaction) {
        n_participants_ = n_participants;
        transaction_ = transaction;
        commit_timestamp_ = std::max(commit_timestamp_, start_timestamp);
    }

    void PrepareOK(int shard_idx, Timestamp &prepare_timestamp) {
        commit_timestamp_ = std::max(commit_timestamp_, prepare_timestamp);
        ok_participants_.insert(shard_idx);
    }

    bool TryCoord() {
        return n_participants_ != -1 &&
               ok_participants_.size() ==
                   (static_cast<std::size_t>(n_participants_) - 1);
    }

    bool CanCommit() {
        return n_participants_ != -1 &&
               ok_participants_.size() ==
                   static_cast<std::size_t>(n_participants_);
    }

    Timestamp GetTimeCommit() { return commit_timestamp_; }

   private:
    Timestamp commit_timestamp_;
    int n_participants_;
    std::unordered_set<int> ok_participants_;
    Transaction transaction_;
};

class Coordinator {
   public:
    Coordinator(const TrueTime &tt);
    ~Coordinator();

    Transaction GetTransaction(uint64_t transaction_id);

    int GetNParticipants(uint64_t transaction_id);

    Decision StartTransaction(uint64_t client_id, uint64_t transaction_id,
                              int n_participants, Transaction transaction);

    CommitDecision ReceivePrepareOK(uint64_t transaction_id, int shard_idx,
                                    Timestamp &prepare_timestamp);

    void Commit(uint64_t transaction_id);

    void Abort(uint64_t transaction_id);

    uint64_t CommitWaitMs(Timestamp &commit_timestamp);

   private:
    const TrueTime &tt_;
    std::unordered_map<uint64_t, PreparedTransaction> prepared_transactions_;
    std::unordered_set<uint64_t> aborted_transactions_;
};

}  // namespace strongstore

#endif /* _STRONG_COORDINATOR_H_ */
