
#include "store/strongstore/coordinator.h"

namespace strongstore
{

    Coordinator::Coordinator(TrueTime &tt) : tt{tt}, preparedTransactions_{}, abortedTransactions_{}
    {
    }

    Coordinator::~Coordinator()
    {
    }

    std::unordered_set<replication::RequestID> Coordinator::GetRequestIDs(uint64_t txnID)
    {
        if (abortedTransactions_.find(txnID) != abortedTransactions_.end())
        {
            return {};
        }

        auto search = preparedTransactions_.find(txnID);
        ASSERT(search != preparedTransactions_.end());
        return search->second.GetRequestIDs();
    }

    Transaction Coordinator::GetTransaction(uint64_t txnID)
    {
        auto search = preparedTransactions_.find(txnID);
        ASSERT(search != preparedTransactions_.end());
        return search->second.GetTransaction();
    }

    int Coordinator::GetNParticipants(uint64_t txnID)
    {
        auto search = preparedTransactions_.find(txnID);
        ASSERT(search != preparedTransactions_.end());
        return search->second.GetNParticipants();
    }

    Decision Coordinator::StartTransaction(replication::RequestID requestID, uint64_t txnID, int nParticipants, Transaction transaction)
    {
        uint64_t timeStart;
        uint64_t error;
        tt.GetTimeAndError(timeStart, error);
        timeStart += error;
        Debug("Coordinator: StartTransaction %lu %lu %lu %lu %d", requestID.clientID, requestID.requestID, txnID, timeStart, nParticipants);
        if (abortedTransactions_.find(txnID) != abortedTransactions_.end())
        {
            return Decision::ABORT;
        }

        auto search = preparedTransactions_.find(txnID);
        if (search == preparedTransactions_.end())
        {
            preparedTransactions_.insert({txnID, {requestID, timeStart, nParticipants, transaction}});
            search = preparedTransactions_.find(txnID);
        }
        else
        {
            search->second.SetTimeStart(timeStart);
            search->second.SetNParticipants(nParticipants);
            search->second.SetTransaction(transaction);
            search->second.AddRequestID(requestID);
        }

        if (search->second.TryCoord())
        {
            return Decision::TRY_COORD;
        }
        else
        {
            return Decision::WAIT;
        }
    }

    CommitDecision Coordinator::ReceivePrepareOK(replication::RequestID requestID, uint64_t txnID, int shardID, uint64_t timePrepare)
    {
        if (abortedTransactions_.find(txnID) != abortedTransactions_.end())
        {
            return {Decision::ABORT, 0};
        }

        auto search = preparedTransactions_.find(txnID);
        if (search == preparedTransactions_.end())
        {
            preparedTransactions_.insert({txnID, {requestID, shardID}});
            search = preparedTransactions_.find(txnID);
        }

        search->second.PrepareOK(requestID, shardID, timePrepare);

        if (search->second.CanCommit())
        {
            return {Decision::COMMIT, search->second.GetTimeCommit()};
        }
        else if (search->second.TryCoord())
        {
            return {Decision::TRY_COORD, 0};
        }
        else
        {
            return {Decision::WAIT, 0};
        }
    }

    void Coordinator::Commit(uint64_t txnID)
    {
        preparedTransactions_.erase(txnID);
    }

    void Coordinator::Abort(uint64_t txnID)
    {
        abortedTransactions_.insert(txnID);
        preparedTransactions_.erase(txnID);
    }

};