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
    : transport(transport), client_id(client_id), shard(shard) {
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

    blockingBegin = nullptr;
}

ShardClient::~ShardClient() { delete client; }

/* Sends BEGIN to a single shard indexed by i. */
void ShardClient::Begin(uint64_t id) {
    Debug("[shard %i] BEGIN: %lu", shard, id);

    // Wait for any previous pending requests.
    if (blockingBegin != nullptr) {
        blockingBegin->GetReply();
        delete blockingBegin;
        blockingBegin = nullptr;
    }
}

/* Returns the value corresponding to the supplied key. */
void ShardClient::Get(uint64_t id, const string &key, Promise *promise) {
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%s]", shard, key.c_str());

    // create request
    string request_str;
    Request request;
    request.set_op(Request::GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    request.SerializeToString(&request_str);

    // set to 1 second by default
    int timeout = (promise != nullptr) ? promise->GetTimeout() : 1000;

    transport->Timer(0, [=]() {
        client->InvokeUnlogged(replica, request_str,
                               bind(&ShardClient::GetCallback, this, promise,
                                    placeholders::_1, placeholders::_2),
                               bind(&ShardClient::GetTimeout, this, promise),
                               timeout);  // timeout in ms
    });
}

void ShardClient::Get(uint64_t id, const string &key,
                      const Timestamp &timestamp, Promise *promise) {
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

    // set to 1 second by default
    int timeout = (promise != nullptr) ? promise->GetTimeout() : 1000;

    transport->Timer(0, [=]() {
        client->InvokeUnlogged(replica, request_str,
                               bind(&ShardClient::GetCallback, this, promise,
                                    placeholders::_1, placeholders::_2),
                               bind(&ShardClient::GetTimeout, this, promise),
                               timeout);  // timeout in ms
    });
}

void ShardClient::Put(uint64_t id, const string &key, const string &value,
                      Promise *promise) {
    Panic("Unimplemented PUT");
    return;
}

void ShardClient::Prepare(uint64_t id, const Transaction &txn, int coordShard,
                          int nParticipants, Promise *promise) {
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

    transport->Timer(0, [=]() {
        client->Invoke(request_str,
                       bind(&ShardClient::PrepareCallback, this, promise,
                            placeholders::_1, placeholders::_2));
    });
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

void ShardClient::Commit(int coordShard, uint64_t id, uint64_t timestamp) {
    Debug("[shard %i] Sending COMMIT: %lu", shard, id);

    // create commit request
    string request_str;
    Request request;
    request.set_op(Request::COMMIT);
    request.set_txnid(id);
    request.mutable_commit()->set_timestamp(timestamp);
    request.mutable_commit()->set_coordinatorshard(coordShard);
    request.SerializeToString(&request_str);

    blockingBegin = new Promise(COMMIT_TIMEOUT);
    transport->Timer(0, [=]() {
        client->Invoke(request_str, bind(&ShardClient::CommitCallback, this,
                                         placeholders::_1, placeholders::_2));
    });
}

/* Aborts the ongoing transaction. */
void ShardClient::Abort(int coordShard, uint64_t id) {
    Debug("[shard %i] Sending ABORT: %lu", shard, id);

    // create abort request
    string request_str;
    Request request;
    request.set_op(Request::ABORT);
    request.set_txnid(id);
    request.mutable_abort()->set_coordinatorshard(coordShard);
    request.SerializeToString(&request_str);

    blockingBegin = new Promise(ABORT_TIMEOUT);
    transport->Timer(0, [=]() {
        client->Invoke(request_str, bind(&ShardClient::AbortCallback, this,
                                         placeholders::_1, placeholders::_2));
    });
}

void ShardClient::GetTimeout(Promise *promise) {
    if (promise != nullptr) {
        promise->Reply(REPLY_TIMEOUT);
    }
}

/* Callback from a shard replica on get operation completion. */
void ShardClient::GetCallback(Promise *promise, const string &request_str,
                              const string &reply_str) {
    /* Replies back from a shard. */
    Reply reply;
    reply.ParseFromString(reply_str);

    Debug("[shard %i] Received GET callback [%d]", shard, reply.status());
    if (promise != nullptr) {
        if (reply.has_timestamp()) {
            promise->Reply(reply.status(), Timestamp(reply.timestamp()),
                           reply.value());
        } else {
            promise->Reply(reply.status(), reply.value());
        }
    }
}

/* Callback from a shard replica on prepare operation completion. */
void ShardClient::PrepareCallback(Promise *promise, const string &request_str,
                                  const string &reply_str) {
    Reply reply;

    reply.ParseFromString(reply_str);
    Debug("[shard %i] Received COMMIT callback [%d]", shard, reply.status());

    if (promise != nullptr) {
        switch (reply.status()) {
            case REPLY_OK:
                ASSERT(reply.has_timestamp());
                Debug("[shard %i] COMMIT timestamp [%lu]", shard,
                      reply.timestamp());
                promise->Reply(reply.status(), Timestamp(reply.timestamp(), 0));
                break;
            case REPLY_FAIL:
                promise->Reply(reply.status());
                break;
            default:
                NOT_REACHABLE();
        }
    }
}

/* Callback from a shard replica on commit operation completion. */
void ShardClient::CommitCallback(const string &request_str,
                                 const string &reply_str) {
    // COMMITs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    ASSERT(reply.status() == REPLY_OK);

    Debug("[shard %i] Received COMMIT callback [%d]", shard, reply.status());
}

/* Callback from a shard replica on abort operation completion. */
void ShardClient::AbortCallback(const string &request_str,
                                const string &reply_str) {
    // ABORTs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    ASSERT(reply.status() == REPLY_OK);

    Debug("[shard %i] Received ABORT callback [%d]", shard, reply.status());
}

}  // namespace strongstore
