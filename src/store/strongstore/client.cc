// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/client.cc:
 *   Client to transactional storage system with strong consistency
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

#include "store/strongstore/client.h"

#include "lib/latency.h"

using namespace std;

namespace strongstore {

Client::Client(Mode mode, transport::Configuration *config, uint64_t id,
               uint64_t nshards, int closestReplica, Transport *transport,
               Partitioner *part)
    : config(config),
      client_id(id),
      nshards(nshards),
      transport(transport),
      mode(mode),
      part(part) {
    t_id = client_id << 26;

    Debug("Initializing StrongStore client with id [%lu]", client_id);

    /* Start a client for each shard. */
    for (uint64_t i = 0; i < nshards; i++) {
        ShardClient *shardclient = new ShardClient(
            mode, config, transport, client_id, i, closestReplica);
        bclient.push_back(new BufferClient(shardclient));
        sclient.push_back(shardclient);
    }

    Debug("SpanStore client [%lu] created!", client_id);
    _Latency_Init(&opLat, "op_lat");
}

Client::~Client() {
    Latency_Dump(&opLat);
    for (auto b : bclient) {
        delete b;
    }
    for (auto s : sclient) {
        delete s;
    }
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void Client::Begin() {
    Debug("BEGIN Transaction");
    t_id++;
    participants.clear();
    for (uint64_t i = 0; i < nshards; i++) {
        bclient[i]->Begin(t_id);
    }
}

/* Returns the value corresponding to the supplied key. */
int Client::Get(const string &key, string &value) {
    // Contact the appropriate shard to get the value.
    std::vector<int> txnGroups(participants.begin(), participants.end());
    int i = (*part)(key, nshards, -1, txnGroups);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
    }

    // Send the GET operation to appropriate shard.
    Promise promise(GET_TIMEOUT);

    bclient[i]->Get(key, &promise);
    value = promise.GetValue();

    return promise.GetReply();
}

/* Sets the value corresponding to the supplied key. */
int Client::Put(const string &key, const string &value) {
    // Contact the appropriate shard to set the value.
    std::vector<int> txnGroups(participants.begin(), participants.end());
    int i = (*part)(key, nshards, -1, txnGroups);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
    }

    Promise promise(PUT_TIMEOUT);

    // Buffering, so no need to wait.
    bclient[i]->Put(key, value, &promise);
    return promise.GetReply();
}

int Client::ChooseCoordinator(const std::set<int> &participants) {
    ASSERT(participants.size() != 0);
    // TODO: Choose coordinator
    return *participants.begin();
}

int Client::Prepare(uint64_t &ts) {
    int status;

    // 1. Send commit-prepare to all shards.
    Debug("PREPARE Transaction");
    Promise promise(PREPARE_TIMEOUT);

    int coordShard = ChooseCoordinator(participants);
    int nParticipants = participants.size();

    for (auto p : participants) {
        Debug("Sending prepare to shard [%d]", p);
        if (p == coordShard) {
            bclient[p]->Prepare(coordShard, nParticipants, &promise);
        } else {
            bclient[p]->Prepare(coordShard, nParticipants);
        }
    }

    // 2. Wait for reply from all shards. (abort on timeout)
    Debug("Waiting for PREPARE replies");

    status = promise.GetReply();
    return status;
}

/* Attempts to commit the ongoing transaction. */
bool Client::Commit() {
    // Implementing 2 Phase Commit
    uint64_t ts = 0;
    int status = Prepare(ts);

    if (status == REPLY_OK) {
        return true;
    } else {
        return false;
    }
}

/* Aborts the ongoing transaction. */
void Client::Abort() { Panic("Unimplemented: ABORT Transaction"); }

/* Return statistics of most recent transaction. */
vector<int> Client::Stats() {
    vector<int> v;
    return v;
}

}  // namespace strongstore
