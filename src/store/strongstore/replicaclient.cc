#include "store/strongstore/replicaclient.h"

#include "lib/configuration.h"

namespace strongstore {

using namespace std;
using namespace proto;

ReplicaClient::ReplicaClient(const transport::Configuration &config,
                             Transport *transport, uint64_t client_id,
                             int shard)
    : config_{config},
      transport_{transport},
      client_id_(client_id),
      shard_idx_(shard),
      pendingCommits{},
      lastReqId{0} {
    client = new replication::vr::VRClient(config_, transport_, shard_idx_,
                                           client_id_);
}

ReplicaClient::~ReplicaClient() { delete client; }

void ReplicaClient::FastPathCommit(uint64_t transaction_id,
                                   const Transaction transaction,
                                   uint64_t commit_timestamp,
                                   commit_callback ccb,
                                   commit_timeout_callback ctcb,
                                   uint32_t timeout) {
    Debug("[shard %i] Sending fast path COMMIT: %lu", shard_idx_,
          transaction_id);

    // create commit request
    string request_str;
    Request request;
    request.set_op(Request::COMMIT);
    request.set_txnid(transaction_id);
    request.mutable_commit()->set_timestamp(commit_timestamp);
    transaction.serialize(request.mutable_commit()->mutable_transaction());
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingCommit *pendingCommit = new PendingCommit(reqId);
    pendingCommits[reqId] = pendingCommit;
    pendingCommit->ccb = ccb;
    pendingCommit->ctcb = ctcb;

    client->Invoke(request_str, bind(&ReplicaClient::CommitCallback, this,
                                     pendingCommit->reqId, placeholders::_1,
                                     placeholders::_2));
}

void ReplicaClient::Commit(uint64_t transaction_id, uint64_t commit_timestamp,
                           commit_callback ccb, commit_timeout_callback ctcb,
                           uint32_t timeout) {
    Debug("[shard %i] Sending COMMIT: %lu", shard_idx_, transaction_id);

    // create commit request
    string request_str;
    Request request;
    request.set_op(Request::COMMIT);
    request.set_txnid(transaction_id);
    request.mutable_commit()->set_timestamp(commit_timestamp);
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingCommit *pendingCommit = new PendingCommit(reqId);
    pendingCommits[reqId] = pendingCommit;
    pendingCommit->ccb = ccb;
    pendingCommit->ctcb = ctcb;

    client->Invoke(request_str, bind(&ReplicaClient::CommitCallback, this,
                                     pendingCommit->reqId, placeholders::_1,
                                     placeholders::_2));
}

/* Callback from a shard replica on commit operation completion. */
bool ReplicaClient::CommitCallback(uint64_t reqId, const string &request_str,
                                   const string &reply_str) {
    // COMMITs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    ASSERT(reply.status() == REPLY_OK);

    Debug("[shard %i] Received COMMIT callback [%d]", shard_idx_,
          reply.status());

    auto itr = this->pendingCommits.find(reqId);
    ASSERT(itr != pendingCommits.end());
    PendingCommit *pendingCommit = itr->second;
    commit_callback ccb = pendingCommit->ccb;
    this->pendingCommits.erase(itr);
    delete pendingCommit;
    ccb(COMMITTED);

    return true;
}

}  // namespace strongstore
