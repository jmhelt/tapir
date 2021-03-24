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

Client::Client(Consistency consistency, transport::Configuration &config,
               uint64_t client_id, int nShards, int closestReplica,
               Transport *transport, Partitioner *part, TrueTime &tt,
               bool debug_stats)
    : min_read_timestamp_{},
      config_{config},
      client_id_{client_id},
      nshards_(nShards),
      transport_{transport},
      part(part),
      tt_{tt},
      consistency_{consistency},
      debug_stats_{debug_stats},
      ping_replicas_{true},
      first_{true} {
    t_id = client_id_ << 26;

    Debug("Initializing StrongStore client with id [%lu]", client_id_);

    /* Start a client for each shard. */
    for (uint64_t i = 0; i < nshards_; i++) {
        ShardClient *shardclient =
            new ShardClient(config_, transport_, client_id_, i);
        bclient.push_back(new BufferClient(shardclient));
        sclient.push_back(shardclient);
    }

    Debug("SpanStore client [%lu] created!", client_id_);

    if (debug_stats_) {
        _Latency_Init(&op_lat_, "op_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }
}

Client::~Client() {
    if (debug_stats_) {
        Latency_Dump(&op_lat_);
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
    if (debug_stats_) {
        Latency_Start(&op_lat_);
    }
    transport_->Timer(0, [this, bcb, btcb, timeout]() {
        if (ping_replicas_ && first_) {
            for (uint64_t i = 0; i < nshards_; i++) {
                sclient[i]->StartPings();
            }
            first_ = false;
        }

        Debug("BEGIN [%lu]", t_id + 1);
        t_id++;
        participants_.clear();

        Timestamp start_time{tt_.GetTime(), client_id_};
        for (uint64_t i = 0; i < nshards_; i++) {
            bclient[i]->Begin(t_id, start_time);
        }
        bcb(t_id);
    });
}

/* Returns the value corresponding to the supplied key. */
void Client::Get(const std::string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout) {
    transport_->Timer(0, [this, key, gcb, gtcb, timeout]() {
        Debug("GET [%lu : %s]", t_id, BytesToHex(key, 16).c_str());
        // Contact the appropriate shard to get the value.
        int i = (*part)(key, nshards_, -1, participants_);

        // If needed, add this shard to set of participants
        if (participants_.find(i) == participants_.end()) {
            participants_.insert(i);
        }

        // Send the GET operation to appropriate shard.
        auto gcbLat = [this, gcb](int status, const std::string &key,
                                  const std::string &val,
                                  Timestamp ts) { gcb(status, key, val, ts); };
        bclient[i]->Get(key, gcbLat, gtcb, timeout);
    });
}

/* Sets the value corresponding to the supplied key. */
void Client::Put(const std::string &key, const std::string &value,
                 put_callback pcb, put_timeout_callback ptcb,
                 uint32_t timeout) {
    transport_->Timer(0, [this, key, value, pcb, ptcb, timeout]() {
        Debug("PUT [%lu : %s]", t_id, key.c_str());
        // Contact the appropriate shard to set the value.
        int i = (*part)(key, nshards_, -1, participants_);

        // If needed, add this shard to set of participants
        if (participants_.find(i) == participants_.end()) {
            participants_.insert(i);
        }

        auto pcbLat = [this, pcb](int status1, const std::string &key1,
                                  const std::string &val1) {
            pcb(status1, key1, val1);
        };
        bclient[i]->Put(key, value, pcbLat, ptcb, timeout);
    });
}

int Client::ChooseCoordinator() {
    ASSERT(participants_.size() != 0);

    // Choose farthest coordinator
    uint64_t max_lat = 0;
    int max_p = -1;
    for (int p : participants_) {
        uint64_t lat = sclient[p]->GetLatencyToLeader();
        if (lat >= max_lat) {
            max_lat = lat;
            max_p = p;
        }
    }

    Debug("Chosen coordinator: %d", max_p);

    return max_p;
}

Timestamp Client::ChooseNonBlockTimestamp() {
    return {tt_.GetTime() + 10000, client_id_};
}

void Client::Prepare(PendingRequest *req, uint32_t timeout) {
    Debug("PREPARE [%lu]", t_id);
    ASSERT(participants_.size() > 0);

    req->outstandingPrepares = 0;
    req->prepareStatus = REPLY_OK;
    req->maxRepliedTs = 0UL;

    int coordinator_shard = ChooseCoordinator();
    int n_participants = participants_.size();

    Timestamp nonblock_timestamp = Timestamp();
    if (consistency_ == Consistency::RSS) {
        nonblock_timestamp = ChooseNonBlockTimestamp();
        req->nonblock_timestamp = nonblock_timestamp;
    }

    for (auto p : participants_) {
        if (p == coordinator_shard) {
            bclient[p]->RWCommitCoordinator(
                t_id, n_participants, nonblock_timestamp,
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                timeout);
        } else {
            bclient[p]->RWCommitParticipant(
                t_id, coordinator_shard, nonblock_timestamp,
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
                },
                [](int, Timestamp) {}, timeout);
        }
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
    Timestamp nonblock_timestamp = req->nonblock_timestamp;
    pendingReqs.erase(reqId);
    delete req;

    if (consistency_ == Consistency::SS) {
        if (debug_stats_) {
            Latency_End(&commit_lat_);
        }
        ccb(tstatus);
    } else if (consistency_ == Consistency::RSS) {
        uint64_t ms = tt_.TimeToWaitUntilMS(nonblock_timestamp.getTimestamp());
        Debug("Waiting for nonblock time: %lu", ms);
        min_read_timestamp_ = std::max(min_read_timestamp_, respTs);
        Debug("min_read_timestamp_: %lu.%lu",
              min_read_timestamp_.getTimestamp(), min_read_timestamp_.getID());
        transport_->Timer(ms, [this, ccb, tstatus] {
            if (debug_stats_) {
                Latency_End(&commit_lat_);
            }
            ccb(tstatus);
        });
    } else {
        NOT_REACHABLE();
    }
}

/* Attempts to commit the ongoing transaction. */
void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
                    uint32_t timeout) {
    if (debug_stats_) {
        Latency_End(&op_lat_);
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

    stats.IncrementList("txn_groups", participants_.size());

    Prepare(req, timeout);
}

/* Aborts the ongoing transaction. */
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
                   uint32_t timeout) {
    Panic("Unimplemented ABORT");
}

/* Commits RO transaction. */
void Client::ROCommit(const std::unordered_set<std::string> &keys,
                      commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) {
    t_id++;
    participants_.clear();

    Timestamp commit_timestamp{tt_.Now().latest(), client_id_};

    std::unordered_map<int, std::vector<std::string>> sharded_keys;

    for (auto &key : keys) {
        int i = (*part)(key, nshards_, -1, participants_);
        sharded_keys[i].push_back(key);
        // auto search = participants_.find(i);
        // if (search == participants_.end()) {
        //     bclient[i]->Begin(t_id, commit_timestamp);
        //     participants_.insert(i);
        // }

        // bclient[i]->AddReadSet(key, commit_timestamp);
    }

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
    req->outstandingPrepares = sharded_keys.size();
    req->prepareStatus = REPLY_OK;

    stats.IncrementList("txn_groups", participants_.size());

    ASSERT(participants_.size() > 0);

    for (auto p : participants_) {
        // TODO: Handle timeout
        bclient[p]->ROCommit(
            t_id, sharded_keys[p], commit_timestamp,
            std::bind(&Client::ROCommitCallback, this, req->id,
                      std::placeholders::_1),
            []() {}, timeout);
    }
}

void Client::ROCommitCallback(uint64_t reqId, transaction_status_t status) {
    Debug("ROCommit [%lu] callback status %d", t_id, status);

    auto itr = this->pendingReqs.find(reqId);
    if (itr == this->pendingReqs.end()) {
        Debug(
            "ROCommitCallback for terminated request id %lu (txn already "
            "committed or aborted.",
            reqId);
        return;
    }

    PendingRequest *req = itr->second;
    --req->outstandingPrepares;
    if (req->outstandingPrepares == 0) {
        commit_callback ccb = req->ccb;
        pendingReqs.erase(reqId);
        delete req;

        if (debug_stats_) {
            Latency_End(&commit_lat_);
        }
        Debug("COMMIT [%lu] OK", t_id);
        ccb(COMMITTED);
    }
}

}  // namespace strongstore
