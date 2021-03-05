
#include "store/strongstore/intershardclient.h"

#include <functional>
#include <random>

using namespace std;

namespace strongstore
{

    InterShardClient::InterShardClient(transport::Configuration &config, Transport *transport, int nShards)
        : nShards(nShards)
    {
        // Initialize all state here;
        clientID = 0;
        while (clientID == 0)
        {
            random_device rd;
            mt19937_64 gen(rd());
            uniform_int_distribution<uint64_t> dis;
            clientID = dis(gen);
        }

        for (int i = 0; i < nShards; i++)
        {
            sclient.push_back(new ShardClient(strongstore::MODE_SPAN_LOCK, &config, transport, clientID, i, -1));
        }
    }

    InterShardClient::~InterShardClient()
    {
        for (auto s : sclient)
        {
            delete s;
        }
    }

    void InterShardClient::PrepareOK(int coordShard, uint64_t txnID, int participantShard, uint64_t prepareTS)
    {
        Debug("PrepareOK: %d %lu %d %lu", coordShard, txnID, participantShard, prepareTS);

        sclient[coordShard]->PrepareOK(txnID, participantShard, prepareTS,
                    std::bind(&InterShardClient::PrepareOKCallback,
                                this,
                                coordShard,
                                txnID,
                                participantShard,
                                placeholders::_1,
                                placeholders::_2));
    }

    void InterShardClient::PrepareOKCallback(int coordShard, uint64_t txnID, int participantShard, const string &request_str, const string &reply_str)
    {
        proto::Reply reply;
        reply.ParseFromString(reply_str);
        Debug("[shard %i] Received PREPARE_OK callback [%d]", participantShard, reply.status());

        switch (reply.status()) {
        case REPLY_OK:
            ASSERT(reply.has_timestamp());
            sclient[participantShard]->Commit(coordShard, txnID, reply.timestamp());
            break;
        case REPLY_FAIL:
            sclient[participantShard]->Abort(coordShard, txnID);
            break;
        default:
            NOT_REACHABLE();
        }
    }

    void InterShardClient::PrepareAbort(int coordShard, uint64_t txnID, int participantShard)
    {
        Debug("PrepareAbort: %d %lu %d", coordShard, txnID, participantShard);

        sclient[coordShard]->PrepareAbort(txnID, participantShard,
                    std::bind(&InterShardClient::PrepareAbortCallback,
                                this,
                                coordShard,
                                txnID,
                                participantShard,
                                placeholders::_1,
                                placeholders::_2));
    }

    void InterShardClient::PrepareAbortCallback(int coordShard, uint64_t txnID, int participantShard, const string &request_str, const string &reply_str)
    {
        proto::Reply reply;
        reply.ParseFromString(reply_str);
        Debug("[shard %i] Received PREPARE_ABORT callback [%d]", participantShard, reply.status());
    }
};