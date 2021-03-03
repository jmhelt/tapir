// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _STRONG_COORDINATOR_H_
#define _STRONG_COORDINATOR_H_

#include "lib/message.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"

#include <vector>
#include <unordered_set>
#include <unordered_map>

namespace strongstore
{

    enum Decision
    {
        WAIT = 1,
        TRY_COORD = 2,
        COMMIT = 3,
        ABORT = 4
    };

    struct CommitDecision
    {
        Decision d;
        uint64_t commitTime;
    };

    class PreparedTransaction
    {
    public:
        PreparedTransaction(int shardID) : timeCommit_{0}, nParticipants_{-1}, okParticipants_{{shardID}} {}
        PreparedTransaction(uint64_t timeStart, int nParticipants, Transaction transaction) : timeCommit_{timeStart}, nParticipants_{nParticipants}, okParticipants_{}, transaction_{transaction} {}
        ~PreparedTransaction() {}

        void SetTimeStart(uint64_t timeStart)
        {
            timeCommit_ = std::max(timeCommit_, timeStart);
        }

        void SetNParticipants(int nParticipants)
        {
            nParticipants_ = nParticipants;
        }

        Transaction GetTransaction()
        {
            return transaction_;
        }

        void SetTransaction(Transaction transaction)
        {
            transaction_ = transaction;
        }

        void PrepareOK(int shardID, uint64_t prepareTime)
        {
            timeCommit_ = std::max(timeCommit_, prepareTime);
            okParticipants_.insert(shardID);
        }

        bool TryCoord()
        {
            return nParticipants_ != -1 && okParticipants_.size() == (static_cast<std::size_t>(nParticipants_) - 1);
        }

        bool CanCommit()
        {
            return nParticipants_ != -1 && okParticipants_.size() == static_cast<std::size_t>(nParticipants_);
        }

        uint64_t GetTimeCommit()
        {
            return timeCommit_;
        }

    private:
        uint64_t timeCommit_;
        int nParticipants_;
        std::unordered_set<int> okParticipants_;
        Transaction transaction_;
    };

    class Coordinator
    {
    public:
        Coordinator(TrueTime &tt);
        ~Coordinator();

        Transaction GetTransaction(uint64_t txnID);

        Decision StartTransaction(uint64_t txnID, int nParticipants, Transaction transaction);

        CommitDecision ReceivePrepareOK(uint64_t txnID, int shardID, uint64_t timePrepare);

        void Commit(uint64_t txnID);

        void Abort(uint64_t txnID);

    private:
        TrueTime &tt;
        std::unordered_map<uint64_t, PreparedTransaction> preparedTransactions;
    };

} // namespace strongstore

#endif /* _STRONG_COORDINATOR_H_ */
