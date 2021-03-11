
#include "store/strongstore/intershardclient.h"

#include <functional>
#include <random>

using namespace std;

namespace strongstore {

InterShardClient::InterShardClient(const transport::Configuration &config,
                                   Transport *transport, int nShards)
    : nShards(nShards) {
    // Initialize all state here;
    clientID = 0;
    while (clientID == 0) {
        random_device rd;
        mt19937_64 gen(rd());
        uniform_int_distribution<uint64_t> dis;
        clientID = dis(gen);
    }

    for (int i = 0; i < nShards; i++) {
        sclient.push_back(new ShardClient(config, transport, clientID, i));
    }
}

InterShardClient::~InterShardClient() {
    for (auto s : sclient) {
        delete s;
    }
}

void InterShardClient::PrepareOK(int coordShard, uint64_t txnID,
                                 int participantShard, uint64_t prepareTS) {
    Debug("PrepareOK: %d %lu %d %lu", coordShard, txnID, participantShard,
          prepareTS);

    // sclient[coordShard]->PrepareOK(
    //     txnID, participantShard, prepareTS,
    //     std::bind(&InterShardClient::PrepareOKCallback, this, coordShard,
    //     txnID,
    //               participantShard, placeholders::_1, placeholders::_2));
}

bool InterShardClient::PrepareOKCallback(int coordShard, uint64_t txnID,
                                         int participantShard,
                                         const string &request_str,
                                         const string &reply_str) {
    proto::Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %i] Received PREPARE_OK callback [%d]", participantShard,
          reply.status());

    switch (reply.status()) {
        case REPLY_OK:
            ASSERT(reply.has_timestamp());
            sclient[participantShard]->Commit(
                coordShard, txnID, reply.timestamp(),
                [](transaction_status_t status) {}, []() {}, 1000);
            break;
        case REPLY_FAIL:
            sclient[participantShard]->Abort(
                coordShard, txnID, []() {}, []() {}, 1000);
            break;
        default:
            NOT_REACHABLE();
    }

    return true;
}

void InterShardClient::PrepareAbort(int coordShard, uint64_t txnID,
                                    int participantShard) {
    Debug("PrepareAbort: %d %lu %d", coordShard, txnID, participantShard);

    sclient[coordShard]->PrepareAbort(
        txnID, participantShard,
        std::bind(&InterShardClient::PrepareAbortCallback, this, coordShard,
                  txnID, participantShard, placeholders::_1, placeholders::_2));
}

bool InterShardClient::PrepareAbortCallback(int coordShard, uint64_t txnID,
                                            int participantShard,
                                            const string &request_str,
                                            const string &reply_str) {
    proto::Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %i] Received PREPARE_ABORT callback [%d]", participantShard,
          reply.status());

    return true;
}
};  // namespace strongstore