// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _STRONG_COORDINATOR_H_
#define _STRONG_COORDINATOR_H_

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/message.h"
#include "replication/common/replica.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"

namespace strongstore {

enum Decision { WAIT = 1, TRY_COORD = 2, COMMIT = 3, ABORT = 4 };

struct CommitDecision {
    Decision d;
    uint64_t commitTime;
};

class PreparedTransaction {
   public:
    PreparedTransaction(replication::RequestID requestID, int shardID)
        : timeCommit_{0},
          nParticipants_{-1},
          okParticipants_{{shardID}},
          requestIDs_{{requestID}} {}
    PreparedTransaction(replication::RequestID requestID, uint64_t timeStart,
                        int nParticipants, Transaction transaction)
        : timeCommit_{timeStart},
          nParticipants_{nParticipants},
          okParticipants_{},
          requestIDs_{{requestID}},
          transaction_{transaction} {}

    ~PreparedTransaction() {}

    void SetTimeStart(uint64_t timeStart) {
        timeCommit_ = std::max(timeCommit_, timeStart);
    }

    int GetNParticipants() { return nParticipants_; }

    void SetNParticipants(int nParticipants) { nParticipants_ = nParticipants; }

    std::unordered_set<replication::RequestID> GetRequestIDs() {
        return requestIDs_;
    }

    void AddRequestID(replication::RequestID requestID) {
        requestIDs_.insert(requestID);
    }

    Transaction GetTransaction() { return transaction_; }

    void SetTransaction(Transaction transaction) { transaction_ = transaction; }

    void PrepareOK(replication::RequestID requestID, int shardID,
                   uint64_t prepareTime) {
        timeCommit_ = std::max(timeCommit_, prepareTime);
        okParticipants_.insert(shardID);
        requestIDs_.insert(requestID);
    }

    bool TryCoord() {
        return nParticipants_ != -1 &&
               okParticipants_.size() ==
                   (static_cast<std::size_t>(nParticipants_) - 1);
    }

    bool CanCommit() {
        return nParticipants_ != -1 &&
               okParticipants_.size() ==
                   static_cast<std::size_t>(nParticipants_);
    }

    uint64_t GetTimeCommit() { return timeCommit_; }

   private:
    uint64_t timeCommit_;
    int nParticipants_;
    std::unordered_set<int> okParticipants_;
    std::unordered_set<replication::RequestID> requestIDs_;
    Transaction transaction_;
};

class Coordinator {
   public:
    Coordinator(TrueTime &tt);
    ~Coordinator();

    std::unordered_set<replication::RequestID> GetRequestIDs(uint64_t txnID);

    Transaction GetTransaction(uint64_t txnID);

    int GetNParticipants(uint64_t txnID);

    Decision StartTransaction(replication::RequestID requestID, uint64_t txnID,
                              int nParticipants, Transaction transaction);

    CommitDecision ReceivePrepareOK(replication::RequestID requestID,
                                    uint64_t txnID, int shardID,
                                    uint64_t timePrepare);

    void Commit(uint64_t txnID);

    void Abort(uint64_t txnID);

    uint64_t CommitWaitMs(uint64_t commit_timestamp);

   private:
    TrueTime &tt;
    std::unordered_map<uint64_t, PreparedTransaction> prepared_transactions_;
    std::unordered_set<uint64_t> aborted_transactions_;
};

}  // namespace strongstore

#endif /* _STRONG_COORDINATOR_H_ */
