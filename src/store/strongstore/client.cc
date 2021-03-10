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
#include "store/common/common.h"

using namespace std;

namespace strongstore {

Client::Client(transport::Configuration *config, uint64_t client_id,
               int nShards, int closestReplica, Transport *transport,
               Partitioner *part, TrueTime &tt, bool debug_stats)
    : config_{config},
      client_id_{client_id},
      nshards(nShards),
      transport_{transport},
      part(part),
      tt_{tt},
      debug_stats_{debug_stats} {
    t_id = client_id_ << 26;

    Debug("Initializing StrongStore client with id [%lu]", client_id_);

    /* Start a client for each shard. */
    for (uint64_t i = 0; i < nshards; i++) {
        ShardClient *shardclient =
            new ShardClient(config_, transport_, client_id_, i, closestReplica);
        bclient.push_back(new BufferClient(shardclient));
        sclient.push_back(shardclient);
    }

    Debug("SpanStore client [%lu] created!", client_id_);
    _Latency_Init(&opLat, "op_lat");

    if (debug_stats_) {
        _Latency_Init(&commit_lat_, "commit_lat");
    }
}

Client::~Client() {
    Latency_Dump(&opLat);

    if (debug_stats_) {
        Latency_Dump(&commit_lat_);
    }

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
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
                   uint32_t timeout) {
    transport_->Timer(0, [this, bcb, btcb, timeout]() {
        Debug("BEGIN [%lu]", t_id + 1);
        t_id++;
        participants.clear();

        Timestamp start_time{tt_.GetTime(), client_id_};
        for (uint64_t i = 0; i < nshards; i++) {
            bclient[i]->Begin(t_id, start_time);
        }
        bcb(t_id);
    });
}

/* Returns the value corresponding to the supplied key. */
void Client::Get(const std::string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout) {
    transport_->Timer(0, [this, key, gcb, gtcb, timeout]() {
        Latency_Start(&opLat);
        Debug("GET [%lu : %s]", t_id, BytesToHex(key, 16).c_str());
        // Contact the appropriate shard to get the value.
        std::vector<int> txnGroups(participants.begin(), participants.end());
        int i = (*part)(key, nshards, -1, txnGroups);

        // If needed, add this shard to set of participants and send BEGIN.
        if (participants.find(i) == participants.end()) {
            participants.insert(i);
            // bclient[i]->Begin(t_id);
        }

        // Send the GET operation to appropriate shard.
        auto gcbLat = [this, gcb](int status, const std::string &key,
                                  const std::string &val, Timestamp ts) {
            Latency_End(&opLat);
            gcb(status, key, val, ts);
        };
        bclient[i]->Get(key, gcbLat, gtcb, timeout);
    });
}

/* Sets the value corresponding to the supplied key. */
void Client::Put(const std::string &key, const std::string &value,
                 put_callback pcb, put_timeout_callback ptcb,
                 uint32_t timeout) {
    transport_->Timer(0, [this, key, value, pcb, ptcb, timeout]() {
        Latency_Start(&opLat);
        Debug("PUT [%lu : %s]", t_id, key.c_str());
        // Contact the appropriate shard to set the value.
        std::vector<int> txnGroups(participants.begin(), participants.end());
        int i = (*part)(key, nshards, -1, txnGroups);

        // If needed, add this shard to set of participants and send BEGIN.
        if (participants.find(i) == participants.end()) {
            participants.insert(i);
            // bclient[i]->Begin(t_id);
        }

        auto pcbLat = [this, pcb](int status1, const std::string &key1,
                                  const std::string &val1) {
            Latency_End(&opLat);
            pcb(status1, key1, val1);
        };
        bclient[i]->Put(key, value, pcbLat, ptcb, timeout);
    });
}

int Client::ChooseCoordinator(const std::set<int> &participants) {
    ASSERT(participants.size() != 0);
    // TODO: Choose coordinator
    return *participants.begin();
}

void Client::Prepare(PendingRequest *req, uint32_t timeout) {
    Debug("PREPARE [%lu]", t_id);
    ASSERT(participants.size() > 0);

    req->outstandingPrepares = 0;
    req->prepareStatus = REPLY_OK;
    req->maxRepliedTs = 0UL;

    int coordShard = ChooseCoordinator(participants);
    int nParticipants = participants.size();

    for (auto p : participants) {
        if (p == coordShard) {
            bclient[p]->Prepare(
                req->id, coordShard, nParticipants,
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                timeout);
        } else {
            bclient[p]->Prepare(
                req->id, coordShard, nParticipants,
                [this, tId = t_id, reqId = req->id](int status, Timestamp) {
                    Debug("PREPARE [%lu] callback status %d", tId, status);

                    auto itr = pendingReqs.find(reqId);
                    if (itr == pendingReqs.end()) {
                        Debug(
                            "PrepareCallback for terminated request id %ld "
                            "(txn already "
                            "committed or aborted.",
                            reqId);
                        return;
                    }
                    PendingRequest *req = itr->second;

                    --req->outstandingPrepares;
                },
                [](int, Timestamp) {}, timeout);
        }
        req->outstandingPrepares++;
    }
}

void Client::PrepareCallback(uint64_t reqId, int status, Timestamp respTs) {
    Debug("PREPARE [%lu] callback status %d", t_id, status);

    auto itr = this->pendingReqs.find(reqId);
    if (itr == this->pendingReqs.end()) {
        Debug(
            "PrepareCallback for terminated request id %lu (txn already "
            "committed or aborted.",
            reqId);
        return;
    }
    PendingRequest *req = itr->second;

    --req->outstandingPrepares;
    transaction_status_t tstatus = ABORTED_SYSTEM;
    switch (status) {
        case REPLY_OK:
            Debug("COMMIT [%lu] OK", t_id);
            tstatus = COMMITTED;
            break;
        default:
            // abort!
            Debug("COMMIT [%lu] ABORT", t_id);
            tstatus = ABORTED_SYSTEM;
            break;
    }

    commit_callback ccb = req->ccb;
    pendingReqs.erase(reqId);
    delete req;
    if (debug_stats_) {
        Latency_End(&commit_lat_);
    }
    ccb(tstatus);
}

/* Attempts to commit the ongoing transaction. */
void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
                    uint32_t timeout) {
    transport_->Timer(0, [this, ccb, ctcb, timeout]() {
        if (debug_stats_) {
            Latency_Start(&commit_lat_);
        }
        uint64_t reqId = lastReqId++;
        PendingRequest *req = new PendingRequest(reqId, t_id);
        pendingReqs[reqId] = req;
        req->ccb = ccb;
        req->ctcb = ctcb;
        req->maxRepliedTs = 0;
        req->callbackInvoked = false;
        req->timeout = timeout;

        stats.IncrementList("txn_groups", participants.size());

        Prepare(req, timeout);
    });
}

/* Aborts the ongoing transaction. */
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
                   uint32_t timeout) {
    Panic("Unimplemented ABORT");
}

}  // namespace strongstore
