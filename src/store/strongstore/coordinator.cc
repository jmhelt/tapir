
#include "store/strongstore/coordinator.h"

namespace strongstore
{

    Coordinator::Coordinator(TrueTime &tt) : tt{tt}, preparedTransactions{}
    {
    }

    Coordinator::~Coordinator()
    {
    }

    std::unordered_set<replication::RequestID> Coordinator::GetRequestIDs(uint64_t txnID)
    {
        auto search = preparedTransactions.find(txnID);
        ASSERT(search != preparedTransactions.end());
        return search->second.GetRequestIDs();
    }

    Transaction Coordinator::GetTransaction(uint64_t txnID)
    {
        auto search = preparedTransactions.find(txnID);
        ASSERT(search != preparedTransactions.end());
        return search->second.GetTransaction();
    }

    int Coordinator::GetNParticipants(uint64_t txnID)
    {
        auto search = preparedTransactions.find(txnID);
        ASSERT(search != preparedTransactions.end());
        return search->second.GetNParticipants();
    }

    Decision Coordinator::StartTransaction(replication::RequestID requestID, uint64_t txnID, int nParticipants, Transaction transaction)
    {
        uint64_t timeStart;
        uint64_t error;
        tt.GetTimeAndError(timeStart, error);
        timeStart += error;

        Debug("Coordinator: StartTransaction %lu %lu %lu %lu %d", requestID.clientID, requestID.requestID, txnID, timeStart, nParticipants);
        auto search = preparedTransactions.find(txnID);
        if (search == preparedTransactions.end())
        {
            preparedTransactions.insert({txnID, {requestID, timeStart, nParticipants, transaction}});
            search = preparedTransactions.find(txnID);
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
        auto search = preparedTransactions.find(txnID);
        if (search == preparedTransactions.end())
        {
            preparedTransactions.insert({txnID, {requestID, shardID}});
            search = preparedTransactions.find(txnID);
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
        preparedTransactions.erase(txnID);
    }

    void Coordinator::Abort(uint64_t txnID)
    {
        preparedTransactions.erase(txnID);
    }

};