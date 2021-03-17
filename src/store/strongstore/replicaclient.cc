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

void ReplicaClient::Prepare(uint64_t transaction_id,
                            const Transaction &transaction,
                            prepare_callback pcb, prepare_timeout_callback ptcb,
                            uint32_t timeout) {
    Debug("[shard %i] Sending PREPARE: %lu", shard_idx_, transaction_id);

    // create prepare request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE);
    request.set_txnid(transaction_id);
    transaction.serialize(request.mutable_prepare()->mutable_txn());
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingPrepare *pendingPrepare = new PendingPrepare(reqId);
    pendingPrepares[reqId] = pendingPrepare;
    pendingPrepare->pcb = pcb;
    pendingPrepare->ptcb = ptcb;

    client->Invoke(request_str, bind(&ReplicaClient::PrepareCallback, this,
                                     pendingPrepare->reqId, placeholders::_1,
                                     placeholders::_2));
}

/* Callback from a shard replica on prepare operation completion. */
bool ReplicaClient::PrepareCallback(uint64_t reqId, const string &request_str,
                                    const string &reply_str) {
    Reply reply;

    reply.ParseFromString(reply_str);

    Debug("[shard %i] Received PREPARE callback [%d]", shard_idx_,
          reply.status());
    auto itr = this->pendingPrepares.find(reqId);
    ASSERT(itr != this->pendingPrepares.end());
    PendingPrepare *pendingPrepare = itr->second;
    prepare_callback pcb = pendingPrepare->pcb;
    this->pendingPrepares.erase(itr);
    delete pendingPrepare;
    if (reply.has_timestamp()) {
        Debug("[shard %i] COMMIT timestamp [%lu]", shard_idx_,
              reply.timestamp());
        pcb(reply.status(), Timestamp(reply.timestamp()));
    } else {
        pcb(reply.status(), Timestamp());
    }

    return true;
}

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

    std::unordered_set<uint64_t> notify_ros{reply.notify_ros().begin(),
                                            reply.notify_ros().end()};

    auto itr = this->pendingCommits.find(reqId);
    ASSERT(itr != pendingCommits.end());
    PendingCommit *pendingCommit = itr->second;
    commit_callback ccb = pendingCommit->ccb;
    this->pendingCommits.erase(itr);
    delete pendingCommit;
    ccb(COMMITTED, notify_ros);

    return true;
}

void ReplicaClient::Abort(uint64_t transaction_id, abort_callback acb,
                          abort_timeout_callback atcb, uint32_t timeout) {
    Debug("[shard %i] Sending ABORT: %lu", shard_idx_, transaction_id);

    // create commit request
    string request_str;
    Request request;
    request.set_op(Request::ABORT);
    request.set_txnid(transaction_id);
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingAbort *pendingAbort = new PendingAbort(reqId);
    pendingAborts[reqId] = pendingAbort;
    pendingAbort->acb = acb;
    pendingAbort->atcb = atcb;

    client->Invoke(request_str, bind(&ReplicaClient::AbortCallback, this,
                                     pendingAbort->reqId, placeholders::_1,
                                     placeholders::_2));
}

/* Callback from a shard replica on abort operation completion. */
bool ReplicaClient::AbortCallback(uint64_t reqId, const string &request_str,
                                  const string &reply_str) {
    // ABORTS always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    ASSERT(reply.status() == REPLY_OK);

    Debug("[shard %i] Received ABORT callback [%d]", shard_idx_,
          reply.status());

    std::unordered_set<uint64_t> notify_ros{reply.notify_ros().begin(),
                                            reply.notify_ros().end()};

    auto itr = this->pendingAborts.find(reqId);
    ASSERT(itr != pendingAborts.end());
    PendingAbort *pendingAbort = itr->second;
    abort_callback acb = pendingAbort->acb;
    this->pendingAborts.erase(itr);
    delete pendingAbort;
    acb(notify_ros);

    return true;
}

}  // namespace strongstore
