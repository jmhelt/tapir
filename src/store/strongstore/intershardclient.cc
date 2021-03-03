
#include "store/strongstore/intershardclient.h"

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

        sclient[coordShard]->PrepareOK(txnID, participantShard, prepareTS);
    }
    
    void InterShardClient::PrepareAbort(int coordShard, uint64_t txnID)
    {
        Debug("PrepareAbort: %d %lu", coordShard, txnID);

        sclient[coordShard]->PrepareAbort(txnID);
    }
};