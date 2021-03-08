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

ShardClient::ShardClient(Mode mode, transport::Configuration *config,
                         Transport *transport, uint64_t client_id, int shard,
                         int closestReplica)
    : config(config), transport(transport), client_id(client_id), shard(shard) {
    client =
        new replication::vr::VRClient(*config, transport, shard, client_id);

    if (mode == MODE_OCC || mode == MODE_SPAN_OCC) {
        if (closestReplica == -1) {
            replica = client_id % config->n;
        } else {
            replica = closestReplica;
        }
        Debug("Sending unlogged to replica %i", replica);
    } else {
        replica = 0;
    }
    _Latency_Init(&opLat, "op_lat_server");
}

ShardClient::~ShardClient() {
    Latency_Dump(&opLat);
    delete client;
}

/* Sends BEGIN to a single shard indexed by i. */
void ShardClient::Begin(uint64_t id) {
    Debug("[shard %i] BEGIN: %lu", shard, id);
}

/* Returns the value corresponding to the supplied key. */
void ShardClient::Get(uint64_t id, const std::string &key, get_callback gcb,
                      get_timeout_callback gtcb, uint32_t timeout) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard, key.c_str());

    // create request
    string request_str;
    Request request;
    request.set_op(Request::GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingGet *pendingGet = new PendingGet(reqId);
    pendingGets[reqId] = pendingGet;
    pendingGet->key = key;
    pendingGet->gcb = gcb;
    pendingGet->gtcb = gtcb;

    Latency_Start(&opLat);
    client->InvokeUnlogged(
        replica, request_str,
        bind(&ShardClient::GetCallback, this, pendingGet->reqId,
             std::placeholders::_1, std::placeholders::_2),
        bind(&ShardClient::GetTimeout, this, pendingGet->reqId),
        timeout);  // timeout in ms
}

void ShardClient::Get(uint64_t id, const std::string &key,
                      const Timestamp &timestamp, get_callback gcb,
                      get_timeout_callback gtcb, uint32_t timeout) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard, key.c_str());

    // create request
    string request_str;
    Request request;
    request.set_op(Request::GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    timestamp.serialize(request.mutable_get()->mutable_timestamp());
    request.SerializeToString(&request_str);

    uint64_t reqId = lastReqId++;
    PendingGet *pendingGet = new PendingGet(reqId);
    pendingGets[reqId] = pendingGet;
    pendingGet->key = key;
    pendingGet->gcb = gcb;
    pendingGet->gtcb = gtcb;

    Latency_Start(&opLat);
    client->InvokeUnlogged(
        replica, request_str,
        bind(&ShardClient::GetCallback, this, pendingGet->reqId,
             std::placeholders::_1, std::placeholders::_2),
        bind(&ShardClient::GetTimeout, this, pendingGet->reqId),
        timeout);  // timeout in ms
}

void ShardClient::Put(uint64_t id, const std::string &key,
                      const std::string &value, put_callback pcb,
                      put_timeout_callback ptcb, uint32_t timeout) {
    Panic("Unimplemented PUT");
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
    Debug("[shard %i] Sending PREPARE: %lu", shard, id);

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
    Debug("[shard %i] Sending PREPARE_OK: %lu", shard, id);

    // create prepare_ok request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE_OK);
    request.set_txnid(id);
    request.mutable_prepareok()->set_participantshard(participantShard);
    request.mutable_prepareok()->set_timestamp(prepareTS);
    request.SerializeToString(&request_str);

    transport->Timer(0, [=]() { client->Invoke(request_str, continuation); });
}

void ShardClient::PrepareAbort(
    uint64_t id, int participantShard,
    replication::Client::continuation_t continuation) {
    Debug("[shard %i] Sending PREPARE_ABORT: %lu", shard, id);

    // create prepare_ok request
    string request_str;
    Request request;
    request.set_op(Request::PREPARE_ABORT);
    request.set_txnid(id);
    request.mutable_prepareabort()->set_participantshard(participantShard);
    request.SerializeToString(&request_str);

    transport->Timer(0, [=]() { client->Invoke(request_str, continuation); });
}

void ShardClient::Commit(int coordShard, uint64_t id, uint64_t timestamp,
                         commit_callback ccb, commit_timeout_callback ctcb,
                         uint32_t timeout) {
    Debug("[shard %i] Sending COMMIT: %lu", shard, id);

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
    Debug("[shard %i] Sending ABORT: %lu", shard, id);

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
    Warning("[shard %i] GET[%lu] timeout.", shard, reqId);
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
        Debug("[shard %lu:%i] GET callback [%d] %lx %lu", client_id, shard,
              reply.status(), *((const uint64_t *)key.c_str()),
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

    Debug("[shard %i] Received PREPARE callback [%d]", shard, reply.status());
    auto itr = this->pendingPrepares.find(reqId);
    ASSERT(itr != this->pendingPrepares.end());
    PendingPrepare *pendingPrepare = itr->second;
    prepare_callback pcb = pendingPrepare->pcb;
    this->pendingPrepares.erase(itr);
    delete pendingPrepare;
    switch (reply.status()) {
        case REPLY_OK:
            if (reply.has_timestamp()) {
                Debug("[shard %i] COMMIT timestamp [%lu]", shard,
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

    Debug("[shard %i] Received COMMIT callback [%d]", shard, reply.status());

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

    Debug("[shard %i] Received ABORT callback [%d]", shard, reply.status());

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
