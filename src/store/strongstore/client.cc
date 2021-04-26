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

#include <cmath>
#include <functional>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "store/common/common.h"

using namespace std;

namespace strongstore {

Client::Client(Consistency consistency, const NetworkConfiguration &net_config,
               const std::string &client_region,
               transport::Configuration &config, uint64_t client_id,
               int nShards, int closestReplica, Transport *transport,
               Partitioner *part, TrueTime &tt, bool debug_stats,
               double nb_time_alpha)
    : vf_{consistency},
      coord_choices_{},
      min_lats_{},
      min_read_timestamp_{},
      net_config_{net_config},
      client_region_{client_region},
      config_{config},
      state_{EXECUTING},
      client_id_{client_id},
      nshards_(nShards),
      transport_{transport},
      part(part),
      tt_{tt},
      consistency_{consistency},
      debug_stats_{debug_stats},
      ping_replicas_{false},
      first_{true},
      nb_time_alpha_{nb_time_alpha} {
    t_id = client_id_ << 26;

    Debug("Initializing StrongStore client with id [%lu]", client_id_);

    /* Start a client for each shard. */
    for (uint64_t i = 0; i < nshards_; i++) {
        ShardClient *shardclient = new ShardClient(config_, transport_, client_id_, i,
                                                   std::bind(&Client::HandleWound, this, std::placeholders::_1));
        bclient.push_back(new BufferClient(shardclient));
        sclient.push_back(shardclient);
    }

    Debug("SpanStore client [%lu] created!", client_id_);

    if (debug_stats_) {
        _Latency_Init(&op_lat_, "op_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }

    CalculateCoordinatorChoices();
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

void Client::CalculateCoordinatorChoices() {
    if (static_cast<std::size_t>(config_.n) > MAX_SHARDS) {
        Panic("CalculateCoordinatorChoices doesn't support more than %lu shards.", MAX_SHARDS);
    }

    std::vector<uint16_t> prepare_lats{};
    prepare_lats.reserve(config_.g);
    std::vector<uint16_t> commit_lats{};
    commit_lats.reserve(config_.g);

    for (int i = 0; i < config_.g; i++) {
        const std::string &leader_region = net_config_.GetRegion(i, 0);
        uint16_t min_q_lat = net_config_.GetMinQuorumLatency(i, 0);

        // Calculate prepare lat (including client to participant)
        uint16_t prepare_lat = net_config_.GetOneWayLatency(client_region_, leader_region);
        prepare_lat += min_q_lat;
        prepare_lats[i] = prepare_lat;

        // Calculate commit lat (including coordinator to client)
        uint16_t commit_lat = min_q_lat;
        commit_lat += net_config_.GetOneWayLatency(leader_region, client_region_);
        commit_lats[i] = commit_lat;
    }

    uint8_t s_max = static_cast<uint8_t>(std::pow(2.0, config_.g));

    for (uint8_t s = 1; s < s_max; s++) {
        std::bitset<MAX_SHARDS> shards{s};

        uint16_t min_lat = static_cast<uint16_t>(-1);
        int min_coord = -1;
        for (std::size_t coord_idx = 1; coord_idx <= shards.count(); coord_idx++) {
            // Find coord
            std::size_t coord = -1;
            std::size_t n_test = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                if (shards.test(i)) {
                    n_test++;
                }

                if (n_test == coord_idx) {
                    coord = i;
                    break;
                }
            }

            const std::string &c_leader_region = net_config_.GetRegion(coord, 0);

            // Find max prepare lat
            uint16_t lat = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                uint16_t l = 0;
                if (i == coord) {
                    l = net_config_.GetOneWayLatency(client_region_, c_leader_region);
                } else if (shards.test(i)) {
                    const std::string &p_leader_region = net_config_.GetRegion(i, 0);
                    l = prepare_lats[i] + net_config_.GetOneWayLatency(p_leader_region, c_leader_region);
                }

                lat = std::max(lat, l);
            }

            lat += commit_lats[coord];

            if (lat < min_lat) {
                min_lat = lat;
                min_coord = static_cast<int>(coord);
            }
        }

        // Not in wide area, pick random coordinator
        if (min_lat == 0) {
            std::size_t coord_idx = (client_id_ % shards.count()) + 1;
            Debug("coord_idx: %lu", coord_idx);
            // Find coord
            std::size_t n_test = 0;
            for (std::size_t i = 0; i < MAX_SHARDS; i++) {
                if (shards.test(i)) {
                    n_test++;
                }

                if (n_test == coord_idx) {
                    min_coord = i;
                    break;
                }
            }
            Debug("Choosing random coord: %d", min_coord);
        }

        coord_choices_.emplace(shards, min_coord);
        min_lats_.emplace(shards, min_lat);
    }

    Debug("Printing coord_choices:");
    for (auto &c : coord_choices_) {
        Debug("shards: %s, min_coord: %d", c.first.to_string().c_str(),
              c.second);
    }

    Debug("Printing min_lats:");
    for (auto &c : min_lats_) {
        Debug("shards: %s, min_lat: %u", c.first.to_string().c_str(), c.second);
    }
}

int Client::ChooseCoordinator() {
    ASSERT(participants_.size() != 0);

    std::bitset<MAX_SHARDS> shards;

    for (int p : participants_) {
        shards.set(p);
    }

    ASSERT(coord_choices_.find(shards) != coord_choices_.end());

    return coord_choices_[shards];
}

Timestamp Client::ChooseNonBlockTimestamp() {
    ASSERT(participants_.size() != 0);

    std::bitset<MAX_SHARDS> shards;

    for (int p : participants_) {
        shards.set(p);
    }

    ASSERT(min_lats_.find(shards) != min_lats_.end());

    uint16_t l = min_lats_[shards];
    uint64_t lat = static_cast<uint64_t>(nb_time_alpha_ * l * 1000);
    auto now = tt_.Now();
    Debug("[%lu] lat: %lu, ts: %lu", t_id, lat, now.earliest() + lat);
    return {now.earliest() + lat, client_id_};
}

void Client::HandleWound(uint64_t transaction_id) {
    if (transaction_id != t_id) {
        Debug("[%lu] Wound for wrong tid: %lu", t_id, transaction_id);
        return;
    }

    if (state_ == ABORTED) {
        Debug("[%lu] Already aborted", transaction_id);
        return;
    }

    if (state_ == EXECUTING) {
        Debug("[%lu] Sending aborts", transaction_id);
        Abort([transaction_id]() { Debug("[%lu] Received wound callback", transaction_id); }, []() {}, ABORT_TIMEOUT);
    } else if (state_ == COMMITTING) {
        // Forward wound to coordinator
        Debug("[%lu] Forwarding wound to coordinator", transaction_id);
        int coordinator = ChooseCoordinator();
        sclient[coordinator]->Wound(transaction_id);
    } else {
        NOT_REACHABLE();
    }
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void Client::Begin(bool is_retry, begin_callback bcb,
                   begin_timeout_callback btcb, uint32_t timeout) {
    if (debug_stats_) {
        Latency_Start(&op_lat_);
    }

    if (!is_retry) {
        start_time_ = Timestamp{tt_.Now().latest(), client_id_};
    }

    transport_->Timer(0, [this, bcb, btcb, timeout]() {
        state_ = EXECUTING;

        if (ping_replicas_ && first_) {
            for (uint64_t i = 0; i < nshards_; i++) {
                sclient[i]->StartPings();
            }
            first_ = false;
        }

        Debug("BEGIN [%lu]", t_id + 1);
        t_id++;
        participants_.clear();

        for (uint64_t i = 0; i < nshards_; i++) {
            bclient[i]->Begin(t_id, start_time_);
        }
        bcb(t_id);
    });
}

/* Returns the value corresponding to the supplied key. */
void Client::Get(const std::string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout) {
    transport_->Timer(0, [this, key, gcb, gtcb, timeout]() {
        if (state_ == ABORTED) {
            Debug("[%lu] Already aborted", t_id);
            gcb(REPLY_FAIL, "", "", Timestamp());
            return;
        }

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
        bclient[i]->Get(key, bclient[i]->start_timestamp(), gcbLat, gtcb,
                        timeout);
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

void Client::Prepare(PendingRequest *req, uint32_t timeout) {
    Debug("[%lu] PREPARE", t_id);
    ASSERT(participants_.size() > 0);

    req->outstandingPrepares = 0;
    req->prepareStatus = REPLY_OK;
    req->maxRepliedTs = 0UL;

    int coordinator_shard = ChooseCoordinator();

    Timestamp nonblock_timestamp = Timestamp();
    if (consistency_ == Consistency::RSS) {
        nonblock_timestamp = ChooseNonBlockTimestamp();
        req->nonblock_timestamp = nonblock_timestamp;
    }

    for (auto p : participants_) {
        if (p == coordinator_shard) {
            bclient[p]->RWCommitCoordinator(
                t_id, participants_, nonblock_timestamp,
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                std::bind(&Client::PrepareCallback, this, req->id,
                          std::placeholders::_1, std::placeholders::_2),
                timeout);
        } else {
            bclient[p]->RWCommitParticipant(
                t_id, coordinator_shard, nonblock_timestamp,
                [this, reqId = req->id](int status, Timestamp) {
                    Debug("[%lu] PREPARE callback status %d", t_id, status);

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

    uint64_t ms = 0;
    if (tstatus == COMMITTED && consistency_ == Consistency::RSS) {
        ms = tt_.TimeToWaitUntilMS(nonblock_timestamp.getTimestamp());
        Debug("Waiting for nonblock time: %lu", ms);
        min_read_timestamp_ = std::max(min_read_timestamp_, respTs);
        Debug("min_read_timestamp_: %lu.%lu",
              min_read_timestamp_.getTimestamp(), min_read_timestamp_.getID());
    }

    transport_->Timer(ms, [this, ccb, tstatus] {
        if (debug_stats_) {
            Latency_End(&commit_lat_);
        }
        ccb(tstatus);
    });
}

/* Attempts to commit the ongoing transaction. */
void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
                    uint32_t timeout) {
    if (state_ == ABORTED) {
        Debug("[%lu] Already aborted", t_id);
        ccb(ABORTED_SYSTEM);
        return;
    }

    state_ = COMMITTING;

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
    Debug("[%lu] ABORT", t_id);

    if (state_ == ABORTED) {
        Debug("[%lu] Already aborted", t_id);
        acb();
        return;
    }

    state_ = ABORTED;

    uint64_t reqId = lastReqId++;
    PendingRequest *req = new PendingRequest(reqId, t_id);
    pendingReqs[reqId] = req;
    req->acb = acb;
    req->atcb = atcb;
    req->outstandingPrepares = participants_.size();
    req->timeout = timeout;

    for (int p : participants_) {
        bclient[p]->Abort(
            std::bind(&Client::AbortCallback, this, req->id), []() {}, timeout);
    }
}

void Client::AbortCallback(uint64_t reqId) {
    Debug("[%lu] Abort callback", t_id);

    auto itr = this->pendingReqs.find(reqId);
    if (itr == this->pendingReqs.end()) {
        Debug("AbortCallback for terminated request id %lu", reqId);
        return;
    }

    PendingRequest *req = itr->second;
    --req->outstandingPrepares;
    if (req->outstandingPrepares == 0) {
        abort_callback acb = req->acb;
        pendingReqs.erase(reqId);
        delete req;

        Debug("[%lu] Abort finished", t_id);
        acb();
    }
}

/* Commits RO transaction. */
void Client::ROCommit(const std::unordered_set<std::string> &keys,
                      commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) {
    state_ = COMMITTING;
    t_id++;

    const Timestamp commit_timestamp{tt_.Now().latest(), client_id_};
    Debug("commit_timestamp: %lu.%lu", commit_timestamp.getTimestamp(),
          commit_timestamp.getID());

    participants_.clear();
    std::unordered_map<int, std::vector<std::string>> sharded_keys;
    for (auto &key : keys) {
        int i = (*part)(key, nshards_, -1, participants_);
        sharded_keys[i].push_back(key);
        participants_.insert(i);
    }

    vf_.StartRO(t_id, participants_);

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

    stats.IncrementList("txn_groups", sharded_keys.size());

    ASSERT(sharded_keys.size() > 0);

    for (auto &s : sharded_keys) {
        // TODO: Handle timeout
        bclient[s.first]->ROCommit(
            t_id, s.second, commit_timestamp, min_read_timestamp_,
            std::bind(&Client::ROCommitCallback, this, t_id, req->id,
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3),
            std::bind(&Client::ROCommitSlowCallback, this, t_id, req->id,
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3, std::placeholders::_4),
            []() {},
            timeout);
    }
}

void Client::ROCommitCallback(uint64_t transaction_id, uint64_t reqId, int shard_idx,
                              const std::vector<Value> &values,
                              const std::vector<PreparedTransaction> &prepares) {
    auto itr = this->pendingReqs.find(reqId);
    if (itr == this->pendingReqs.end()) {
        Debug("[%lu] ROCommitCallback for terminated request id %lu", transaction_id, reqId);
        return;
    }

    Debug("[%lu] ROCommit callback", transaction_id);

    SnapshotResult r = vf_.ReceiveFastPath(transaction_id, shard_idx, values, prepares);
    if (r.state == COMMIT) {
        PendingRequest *req = itr->second;

        commit_callback ccb = req->ccb;
        pendingReqs.erase(itr);
        delete req;

        if (debug_stats_) {
            Latency_End(&commit_lat_);
        }

        vf_.CommitRO(transaction_id);

        min_read_timestamp_ = std::max(min_read_timestamp_, r.max_read_ts);
        Debug("min_read_timestamp_: %lu.%lu", min_read_timestamp_.getTimestamp(), min_read_timestamp_.getID());
        Debug("[%lu] COMMIT OK", transaction_id);
        ccb(COMMITTED);

    } else if (r.state == WAIT) {
        Debug("[%lu] Waiting for more RO responses", transaction_id);
    }
}

void Client::ROCommitSlowCallback(uint64_t transaction_id, uint64_t reqId, int shard_idx,
                                  uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit) {
    auto itr = this->pendingReqs.find(reqId);
    if (itr == this->pendingReqs.end()) {
        Debug("[%lu] ROCommitSlowCallback for terminated request id %lu", transaction_id, reqId);
        return;
    }

    Debug("[%lu] ROCommitSlow callback", transaction_id);

    SnapshotResult r = vf_.ReceiveSlowPath(transaction_id, rw_transaction_id, is_commit, commit_ts);
    if (r.state == COMMIT) {
        PendingRequest *req = itr->second;

        commit_callback ccb = req->ccb;
        pendingReqs.erase(itr);
        delete req;

        if (debug_stats_) {
            Latency_End(&commit_lat_);
        }

        vf_.CommitRO(transaction_id);

        min_read_timestamp_ = std::max(min_read_timestamp_, r.max_read_ts);
        Debug("min_read_timestamp_: %lu.%lu",
              min_read_timestamp_.getTimestamp(), min_read_timestamp_.getID());
        Debug("[%lu] COMMIT OK", transaction_id);
        ccb(COMMITTED);

    } else if (r.state == WAIT) {
        Debug("[%lu] Waiting for more RO responses", transaction_id);
    }
}

}  // namespace strongstore
