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

ShardClient::ShardClient(const transport::Configuration &config,
                         Transport *transport, uint64_t client_id, int shard)
    : config_{config},
      transport_{transport},
      client_id_{client_id},
      shard_idx_{shard} {
    transport_->Register(this, config_, -1, -1);

    replica_ = 0;
    _Latency_Init(&opLat, "op_lat_server");
}

ShardClient::~ShardClient() { Latency_Dump(&opLat); }

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
    } else if (type == rw_commit_p_reply_.GetTypeName()) {
        rw_commit_p_reply_.ParseFromString(data);
        HandleRWCommitParticipantReply(rw_commit_p_reply_);
    } else if (type == prepare_ok_reply_.GetTypeName()) {
        prepare_ok_reply_.ParseFromString(data);
        HandlePrepareOKReply(prepare_ok_reply_);
    } else if (type == prepare_abort_reply_.GetTypeName()) {
        prepare_abort_reply_.ParseFromString(data);
        HandlePrepareAbortReply(prepare_abort_reply_);
    } else if (type == ro_commit_reply_.GetTypeName()) {
        ro_commit_reply_.ParseFromString(data);
        HandleROCommitReply(ro_commit_reply_);
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

void ShardClient::ROCommit(uint64_t transaction_id,
                           const Transaction &transaction, commit_callback ccb,
                           commit_timeout_callback ctcb, uint32_t timeout) {
    Debug("[shard %i] Sending ROCommit [%lu]", shard_idx_, transaction_id);

    uint64_t reqId = lastReqId++;
    PendingROCommit *pendingROCommit = new PendingROCommit(reqId);
    pendingROCommits[reqId] = pendingROCommit;
    pendingROCommit->ccb = ccb;
    pendingROCommit->ctcb = ctcb;

    // TODO: Setup timeout
    ro_commit_.mutable_rid()->set_client_id(client_id_);
    ro_commit_.mutable_rid()->set_client_req_id(reqId);
    ro_commit_.set_transaction_id(transaction_id);
    transaction.get_start_time().serialize(
        ro_commit_.mutable_commit_timestamp());
    ro_commit_.clear_keys();
    for (auto &r : transaction.getReadSet()) {
        ro_commit_.add_keys(r.first.c_str());
    }

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, ro_commit_);
}

void ShardClient::HandleROCommitReply(const proto::ROCommitReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingROCommits.find(req_id);
    if (itr == pendingROCommits.end()) {
        Debug("[%d][%lu] ROCommitReply for stale request.", shard_idx_, req_id);
        return;  // stale request
    }

    PendingROCommit *req = itr->second;
    commit_callback ccb = req->ccb;
    pendingROCommits.erase(itr);
    delete req;

    ccb(COMMITTED);
}

void ShardClient::RWCommitCoordinator(
    uint64_t transaction_id, const Transaction &transaction, int n_participants,
    Timestamp &nonblock_timestamp, prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {
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
    nonblock_timestamp.serialize((rw_commit_c_.mutable_nonblock_timestamp()));

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

    Debug("[shard %i] COMMIT timestamp [%lu %lu]", shard_idx_,
          reply.commit_timestamp().timestamp(), reply.commit_timestamp().id());
    pcb(reply.status(), Timestamp(reply.commit_timestamp()));
}

void ShardClient::RWCommitParticipant(
    uint64_t transaction_id, const Transaction &transaction,
    int coordinator_shard, Timestamp &nonblock_timestamp, prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {
    Debug("[shard %i] Sending RWCommitParticipant [%lu]", shard_idx_,
          transaction_id);

    uint64_t reqId = lastReqId++;
    PendingPrepare *pendingPrepare = new PendingPrepare(reqId);
    pendingPrepares[reqId] = pendingPrepare;
    pendingPrepare->pcb = pcb;
    pendingPrepare->ptcb = ptcb;

    // TODO: Setup timeout
    rw_commit_p_.Clear();
    rw_commit_p_.mutable_rid()->set_client_id(client_id_);
    rw_commit_p_.mutable_rid()->set_client_req_id(reqId);
    rw_commit_p_.set_transaction_id(transaction_id);
    transaction.serialize(rw_commit_p_.mutable_transaction());
    rw_commit_p_.set_coordinator_shard(coordinator_shard);
    nonblock_timestamp.serialize((rw_commit_p_.mutable_nonblock_timestamp()));

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, rw_commit_p_);
}

void ShardClient::HandleRWCommitParticipantReply(
    const proto::RWCommitParticipantReply &reply) {
    Debug("[shard %i] Received RWCommitParticipant", shard_idx_);
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

    pcb(reply.status(), Timestamp());
}

void ShardClient::PrepareOK(uint64_t transaction_id, int participant_shard,
                            Timestamp &prepare_timestamp, prepare_callback pcb,
                            prepare_timeout_callback ptcb, uint32_t timeout) {
    Debug("[shard %i] Sending PrepareOK [%lu]", shard_idx_, transaction_id);

    uint64_t reqId = lastReqId++;
    PendingPrepareOK *pendingPrepareOK = new PendingPrepareOK(reqId);
    pendingPrepareOKs[reqId] = pendingPrepareOK;
    pendingPrepareOK->pcb = pcb;
    pendingPrepareOK->ptcb = ptcb;

    // TODO: Setup timeout
    prepare_ok_.mutable_rid()->set_client_id(client_id_);
    prepare_ok_.mutable_rid()->set_client_req_id(reqId);
    prepare_ok_.set_transaction_id(transaction_id);
    prepare_ok_.set_participant_shard(participant_shard);
    prepare_timestamp.serialize(prepare_ok_.mutable_prepare_timestamp());

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_, prepare_ok_);
}

void ShardClient::HandlePrepareOKReply(const proto::PrepareOKReply &reply) {
    Debug("[shard %i] Received PrepareOKReply", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingPrepareOKs.find(req_id);
    if (itr == pendingPrepareOKs.end()) {
        Debug("[%d][%lu] PrepareOKReply for stale request.", shard_idx_,
              req_id);
        return;  // stale request
    }

    PendingPrepareOK *req = itr->second;
    prepare_callback pcb = req->pcb;
    pendingPrepareOKs.erase(itr);
    delete req;

    Debug("[shard %i] COMMIT timestamp [%lu.%lu]", shard_idx_,
          reply.commit_timestamp().timestamp(), reply.commit_timestamp().id());
    pcb(reply.status(), Timestamp(reply.commit_timestamp()));
}

void ShardClient::PrepareAbort(uint64_t transaction_id, int participant_shard,
                               prepare_callback pcb,
                               prepare_timeout_callback ptcb,
                               uint32_t timeout) {
    Debug("[shard %i] Sending PrepareAbort [%lu]", shard_idx_, transaction_id);

    uint64_t reqId = lastReqId++;
    PendingPrepareAbort *pendingPrepareAbort = new PendingPrepareAbort(reqId);
    pendingPrepareAborts[reqId] = pendingPrepareAbort;
    pendingPrepareAbort->pcb = pcb;
    pendingPrepareAbort->ptcb = ptcb;

    // TODO: Setup timeout
    prepare_abort_.mutable_rid()->set_client_id(client_id_);
    prepare_abort_.mutable_rid()->set_client_req_id(reqId);
    prepare_abort_.set_transaction_id(transaction_id);
    prepare_abort_.set_participant_shard(participant_shard);

    Latency_Start(&opLat);

    transport_->SendMessageToReplica(this, shard_idx_, replica_,
                                     prepare_abort_);
}

void ShardClient::HandlePrepareAbortReply(
    const proto::PrepareAbortReply &reply) {
    Debug("[shard %i] Received PrepareAbortReply", shard_idx_);
    uint64_t req_id = reply.rid().client_req_id();

    auto itr = pendingPrepareAborts.find(req_id);
    if (itr == pendingPrepareAborts.end()) {
        Debug("[%d][%lu] PrepareAbortReply for stale request.", shard_idx_,
              req_id);
        return;  // stale request
    }

    PendingPrepareAbort *req = itr->second;
    prepare_callback pcb = req->pcb;
    pendingPrepareAborts.erase(itr);
    delete req;

    pcb(reply.status(), Timestamp());
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
