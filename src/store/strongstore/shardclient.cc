// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/txnstore/shardclient.h:
 *   Single shard transactional client interface.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "store/strongstore/shardclient.h"

#include "lib/configuration.h"

namespace strongstore {

using namespace std;
using namespace proto;

ShardClient::ShardClient(transport::Configuration *config, Transport *transport,
                         uint64_t client_id, int shard, int closestReplica)
    : config_{config},
      transport_{transport},
      client_id_(client_id),
      shard_idx_(shard) {
    transport_->Register(this, *config_, -1, -1);
    client = new replication::vr::VRClient(*config_, transport_, shard_idx_,
                                           client_id_);

    replica_ = 0;
    _Latency_Init(&opLat, "op_lat_server");
}

ShardClient::~ShardClient() {
    Latency_Dump(&opLat);
    delete client;
}

void ShardClient::ReceiveMessage(const TransportAddress &remote,
                                 const std::string &type,
                                 const std::string &data, void *meta_data) {
    Latency_End(&opLat);
    if (type == get_reply_.GetTypeName()) {
        get_reply_.ParseFromString(data);
        HandleGetReply(get_reply_);
    } else if (type == rw_commit_c_reply_.GetTypeName()) {
        rw_commit_c_reply_.ParseFromString(data);
        HandleRWCommitCoordinatorReply(rw_commit_c_reply_);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

/* Sends BEGIN to a single shard indexed by i. */
void ShardClient::Begin(uint64_t id) {
    Debug("[shard %i] BEGIN: %lu", shard_idx_, id);
}

/* Returns the value corresponding to the supplied key. */
void ShardClient::Get(uint64_t transaction_id, const std::string &key,
                      get_callback gcb, get_timeout_callback gtcb,
                      uint32_t timeout) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard_idx_, key.c_str());

    uint64_t reqId = lastReqId++;
    PendingGet *pendingGet = new PendingGet(reqId);
    pendingGets[reqId] = pendingGet;
    pendingGet->key = key;
    pendingGet->gcb = gcb;
    pendingGet->gtcb = gtcb;

    // TODO: Setup timeout
    get_.Clear();
    get_.mutable_rid()->set_client_id(client_id_);
    get_.mutable_rid()->set_client_req_id(reqId);
    get_.set_transaction_id(transaction_id);
    get_.set_key(key);

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, get_);
}

void ShardClient::Get(uint64_t transaction_id, const std::string &key,
                      const Timestamp &timestamp, get_callback gcb,
                      get_timeout_callback gtcb, uint32_t timeout) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard_idx_, key.c_str());

    uint64_t reqId = lastReqId++;
    PendingGet *pendingGet = new PendingGet(reqId);
    pendingGets[reqId] = pendingGet;
    pendingGet->key = key;
    pendingGet->gcb = gcb;
    pendingGet->gtcb = gtcb;

    // TODO: Setup timeout
    get_.Clear();
    get_.mutable_rid()->set_client_id(client_id_);
    get_.mutable_rid()->set_client_req_id(reqId);
    get_.set_transaction_id(transaction_id);
    timestamp.serialize(get_.mutable_timestamp());
    get_.set_key(key);

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, get_);
}

void ShardClient::HandleGetReply(const proto::GetReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingGets.find(req_id);
    if (itr == pendingGets.end()) {
        Debug("[%d][%lu] GetReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingGet *req = itr->second;
    get_callback gcb = req->gcb;
    std::string key = req->key;
    pendingGets.erase(itr);
    delete req;

    Debug("[shard %i] Received GET reply [%s]", shard_idx_, key.c_str());

    gcb(REPLY_OK, key, reply.val(), Timestamp());
}

void ShardClient::Put(uint64_t id, const std::string &key,
                      const std::string &value, put_callback pcb,
                      put_timeout_callback ptcb, uint32_t timeout) {
    Panic("Unimplemented PUT");
}

void ShardClient::RWCommitCoordinator(uint64_t transaction_id,
                                      const Transaction &transaction,
                                      int n_participants, prepare_callback pcb,
                                      prepare_timeout_callback ptcb,
                                      uint32_t timeout) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending RWCommitCoordinator [%lu]", shard_idx_,
          transaction_id);

    uint64_t reqId = lastReqId++;
    PendingPrepare *pendingPrepare = new PendingPrepare(reqId);
    pendingPrepares[reqId] = pendingPrepare;
    pendingPrepare->pcb = pcb;
    pendingPrepare->ptcb = ptcb;

    // TODO: Setup timeout
    rw_commit_c_.Clear();
    rw_commit_c_.mutable_rid()->set_client_id(client_id_);
    rw_commit_c_.mutable_rid()->set_client_req_id(reqId);
    rw_commit_c_.set_transaction_id(transaction_id);
    transaction.serialize(rw_commit_c_.mutable_transaction());
    rw_commit_c_.set_n_participants(n_participants);

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, rw_commit_c_);
}

void ShardClient::HandleRWCommitCoordinatorReply(
    const proto::RWCommitCoordinatorReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingPrepares.find(req_id);
    if (itr == pendingPrepares.end()) {
        Debug("[%d][%lu] RWCommitCoordinatorReply for stale request.",
              shard_idx_, req_id);
        return;  // stale request
    }

    PendingPrepare *req = itr->second;
    prepare_callback pcb = req->pcb;
    pendingPrepares.erase(itr);
    delete req;

    if (reply.has_commit_timestamp()) {
        Debug("[shard %i] COMMIT timestamp [%lu]", shard_idx_,
              reply.commit_timestamp());
        pcb(reply.status(), Timestamp(reply.commit_timestamp()));
    } else {
        pcb(reply.status(), Timestamp());
    }
}

void ShardClient::Prepare(uint64_t id, const Transaction &txn,
                          const Timestamp &timestamp, prepare_callback pcb,
                          prepare_timeout_callback ptcb, uint32_t timeout) {
    Panic("Unimplemented PREPARE");
}

void ShardClient::Commit(uint64_t id, const Transaction &txn,
                         const Timestamp &timestamp, commit_callback ccb,
                         commit_timeout_callback ctcb, uint32_t timeout) {
    Debug("Unimplemented COMMIT");
}

void ShardClient::Abort(uint64_t id, const Transaction &txn, abort_callback acb,
                        abort_timeout_callback atcb, uint32_t timeout) {
    Debug("Unimplemented ABORT");
}

void ShardClient::Prepare(uint64_t id, const Transaction &txn, int coordShard,
                          int nParticipants, prepare_callback pcb,
                          prepare_timeout_callback ptcb, uint32_t timeout) {
    Debug("[shard %i] Sending PREPARE: %lu", shard_idx_, id);

    // create prepare request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE);
    request.set_txnid(id);
    request.mutable_prepare()->set_coordinatorshard(coordShard);
    request.mutable_prepare()->set_nparticipants(nParticipants);
    txn.serialize(request.mutable_prepare()->mutable_txn());
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingPrepare *pendingPrepare = new PendingPrepare(reqId);
    pendingPrepares[reqId] = pendingPrepare;
    pendingPrepare->pcb = pcb;
    pendingPrepare->ptcb = ptcb;

    client->Invoke(request_str, bind(&ShardClient::PrepareCallback, this,
                                     pendingPrepare->reqId, placeholders::_1,
                                     placeholders::_2));
}

void ShardClient::PrepareOK(uint64_t id, int participantShard,
                            uint64_t prepareTS,
                            replication::Client::continuation_t continuation) {
    Debug("[shard %i] Sending PREPARE_OK: %lu", shard_idx_, id);

    // create prepare_ok request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE_OK);
    request.set_txnid(id);
    request.mutable_prepareok()->set_participantshard(participantShard);
    request.mutable_prepareok()->set_timestamp(prepareTS);
    request.SerializeToString(&request_str);

    transport_->Timer(0, [=]() { client->Invoke(request_str, continuation); });
}

void ShardClient::PrepareAbort(
    uint64_t id, int participantShard,
    replication::Client::continuation_t continuation) {
    Debug("[shard %i] Sending PREPARE_ABORT: %lu", shard_idx_, id);

    // create prepare_ok request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE_ABORT);
    request.set_txnid(id);
    request.mutable_prepareabort()->set_participantshard(participantShard);
    request.SerializeToString(&request_str);

    transport_->Timer(0, [=]() { client->Invoke(request_str, continuation); });
}

void ShardClient::Commit(int coordShard, uint64_t id, uint64_t timestamp,
                         commit_callback ccb, commit_timeout_callback ctcb,
                         uint32_t timeout) {
    Debug("[shard %i] Sending COMMIT: %lu", shard_idx_, id);

    // create commit request
    string request_str;
    Request request;
    request.set_op(Request::COMMIT);
    request.set_txnid(id);
    request.mutable_commit()->set_timestamp(timestamp);
    request.mutable_commit()->set_coordinatorshard(coordShard);
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingCommit *pendingCommit = new PendingCommit(reqId);
    pendingCommits[reqId] = pendingCommit;
    pendingCommit->ccb = ccb;
    pendingCommit->ctcb = ctcb;

    client->Invoke(request_str, bind(&ShardClient::CommitCallback, this,
                                     pendingCommit->reqId, placeholders::_1,
                                     placeholders::_2));
}

/* Aborts the ongoing transaction. */
void ShardClient::Abort(int coordShard, uint64_t id, abort_callback acb,
                        abort_timeout_callback atcb, uint32_t timeout) {
    Debug("[shard %i] Sending ABORT: %lu", shard_idx_, id);

    // create abort request
    string request_str;
    Request request;
    request.set_op(Request::ABORT);
    request.set_txnid(id);
    request.mutable_abort()->set_coordinatorshard(coordShard);
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingAbort *pendingAbort = new PendingAbort(reqId);
    pendingAborts[reqId] = pendingAbort;
    pendingAbort->acb = acb;
    pendingAbort->atcb = atcb;

    client->Invoke(request_str,
                   bind(&ShardClient::AbortCallback, this, pendingAbort->reqId,
                        placeholders::_1, placeholders::_2));
}

void ShardClient::GetTimeout(uint64_t reqId) {
    Warning("[shard %i] GET[%lu] timeout.", shard_idx_, reqId);
    auto itr = this->pendingGets.find(reqId);
    if (itr != this->pendingGets.end()) {
        PendingGet *pendingGet = itr->second;
        get_timeout_callback gtcb = pendingGet->gtcb;
        std::string key = pendingGet->key;
        this->pendingGets.erase(itr);
        delete pendingGet;
        gtcb(REPLY_TIMEOUT, key);
    }
}

/* Callback from a shard replica on get operation completion. */
bool ShardClient::GetCallback(uint64_t reqId, const string &request_str,
                              const string &reply_str) {
    Latency_End(&opLat);
    /* Replies back from a shard. */
    Reply reply;
    reply.ParseFromString(reply_str);

    auto itr = this->pendingGets.find(reqId);
    if (itr != this->pendingGets.end()) {
        PendingGet *pendingGet = itr->second;
        get_callback gcb = pendingGet->gcb;
        std::string key = pendingGet->key;
        this->pendingGets.erase(itr);
        delete pendingGet;
        Debug("[shard %lu:%i] GET callback [%d] %lx %lu", client_id_,
              shard_idx_, reply.status(), *((const uint64_t *)key.c_str()),
              reply.timestamp());
        if (reply.has_timestamp()) {
            gcb(reply.status(), key, reply.value(),
                Timestamp(reply.timestamp()));
        } else {
            gcb(reply.status(), key, reply.value(), Timestamp());
        }
    }
    return true;
}

/* Callback from a shard replica on prepare operation completion. */
bool ShardClient::PrepareCallback(uint64_t reqId, const string &request_str,
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
    switch (reply.status()) {
        case REPLY_OK:
            if (reply.has_timestamp()) {
                Debug("[shard %i] COMMIT timestamp [%lu]", shard_idx_,
                      reply.timestamp());
                pcb(reply.status(), Timestamp(reply.timestamp()));
            } else {
                pcb(reply.status(), Timestamp());
            }
            break;
        case REPLY_FAIL:
            pcb(reply.status(), Timestamp());
            break;
        default:
            NOT_REACHABLE();
    }

    return true;
}

/* Callback from a shard replica on commit operation completion. */
bool ShardClient::CommitCallback(uint64_t reqId, const string &request_str,
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

/* Callback from a shard replica on abort operation completion. */
bool ShardClient::AbortCallback(uint64_t reqId, const string &request_str,
                                const string &reply_str) {
    // ABORTs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    ASSERT(reply.status() == REPLY_OK);

    Debug("[shard %i] Received ABORT callback [%d]", shard_idx_,
          reply.status());

    auto itr = this->pendingAborts.find(reqId);
    ASSERT(itr != this->pendingAborts.end());
    PendingAbort *pendingAbort = itr->second;
    abort_callback acb = pendingAbort->acb;
    this->pendingAborts.erase(itr);
    delete pendingAbort;
    acb();

    return true;
}

}  // namespace strongstore
