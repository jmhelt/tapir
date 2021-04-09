// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/server.cc:
 *   Implementation of a single transactional key-value server with strong
 *consistency.
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

#include "store/strongstore/server.h"

#include <algorithm>
#include <unordered_set>

namespace strongstore {

using namespace std;
using namespace proto;
using namespace replication;

Server::Server(Consistency consistency,
               const transport::Configuration &shard_config,
               const transport::Configuration &replica_config,
               uint64_t server_id, int shard_idx, int replica_idx,
               Transport *transport, const TrueTime &tt, bool debug_stats)
    : PingServer(transport),
      store_{consistency},
      shard_config_{shard_config},
      replica_config_{replica_config},
      transport_{transport},
      tt_{tt},
      coordinator{tt_},
      server_id_{server_id},
      min_prepare_timestamp_{},
      shard_idx_{shard_idx},
      replica_idx_{replica_idx},
      consistency_{consistency},
      debug_stats_{debug_stats} {
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);

    for (int i = 0; i < shard_config_.n; i++) {
        shard_clients_.push_back(
            new ShardClient(shard_config_, transport, server_id_, i));
    }

    replica_client_ =
        new ReplicaClient(replica_config_, transport_, server_id_, shard_idx_);

    if (debug_stats_) {
        _Latency_Init(&ro_wait_lat_, "ro_wait_lat");
    }
}

Server::~Server() {
    for (auto s : shard_clients_) {
        delete s;
    }

    delete replica_client_;

    if (debug_stats_) {
        Latency_Dump(&ro_wait_lat_);
    }
}

// Assume GetStats called once before exiting protgram
Stats &Server::GetStats() {
    Stats &s = store_.GetStats();
    stats_.Merge(s);
    return stats_;
}

void Server::ReceiveMessage(const TransportAddress &remote,
                            const std::string &type, const std::string &data,
                            void *meta_data) {
    if (type == get_.GetTypeName()) {
        get_.ParseFromString(data);
        HandleGet(remote, get_);
    } else if (type == rw_commit_c_.GetTypeName()) {
        rw_commit_c_.ParseFromString(data);
        HandleRWCommitCoordinator(remote, rw_commit_c_);
    } else if (type == rw_commit_p_.GetTypeName()) {
        rw_commit_p_.ParseFromString(data);
        HandleRWCommitParticipant(remote, rw_commit_p_);
    } else if (type == prepare_ok_.GetTypeName()) {
        prepare_ok_.ParseFromString(data);
        HandlePrepareOK(remote, prepare_ok_);
    } else if (type == prepare_abort_.GetTypeName()) {
        prepare_abort_.ParseFromString(data);
        HandlePrepareAbort(remote, prepare_abort_);
    } else if (type == ro_commit_.GetTypeName()) {
        ro_commit_.ParseFromString(data);
        HandleROCommit(remote, ro_commit_);
    } else if (type == abort_.GetTypeName()) {
        abort_.ParseFromString(data);
        HandleAbort(remote, abort_);
    } else if (type == ping_.GetTypeName()) {
        ping_.ParseFromString(data);
        HandlePingMessage(this, remote, ping_);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

void Server::HandleGet(const TransportAddress &remote, proto::Get &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    const std::string &key = msg.key();
    const Timestamp timestamp = Timestamp(msg.timestamp());

    Debug("[%lu] Received GET request: %s", transaction_id, key.c_str());

    std::pair<Timestamp, std::string> value;
    int status = store_.Get(transaction_id, timestamp, key, value);
    if (status == REPLY_WAIT) {
        auto reply =
            new PendingGetReply(client_id, client_req_id, remote.clone());
        reply->key = key;
        reply->timestamp = std::move(timestamp);

        pending_get_replies_[msg.transaction_id()] = reply;
    } else {
        get_reply_.mutable_rid()->CopyFrom(msg.rid());
        get_reply_.set_status(status);
        get_reply_.set_key(msg.key());

        if (status == REPLY_OK) {
            get_reply_.set_val(value.second);
            value.first.serialize(get_reply_.mutable_timestamp());
        }

        transport_->SendMessage(this, remote, get_reply_);
    }
}

void Server::ContinueGet(uint64_t transaction_id) {
    auto search = pending_get_replies_.find(transaction_id);
    if (search != pending_get_replies_.end()) {
        PendingGetReply *reply = search->second;

        const std::string &key = reply->key;
        const Timestamp &timestamp = reply->timestamp;

        Debug("[%lu] Continuing GET request %s", transaction_id,
              reply->key.c_str());

        std::pair<Timestamp, std::string> value;
        int status = store_.Get(transaction_id, timestamp, key, value);
        if (status == REPLY_OK || status == REPLY_FAIL) {
            get_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
            get_reply_.mutable_rid()->set_client_req_id(
                reply->rid.client_req_id());
            get_reply_.set_status(status);
            get_reply_.set_key(key);

            if (status == REPLY_OK) {
                get_reply_.set_val(value.second);
                value.first.serialize(get_reply_.mutable_timestamp());
            }

            const TransportAddress *remote = reply->rid.addr();
            transport_->SendMessage(this, *remote, get_reply_);

            delete remote;
            delete reply;
            pending_get_replies_.erase(search);
        } else {  // Keep waiting
            Debug("[%lu] Waiting for conflicting transactions", transaction_id);
        }
    }
}

void Server::ContinueCoordinatorPrepare(uint64_t transaction_id) {
    auto search = pending_rw_commit_c_replies_.find(transaction_id);
    if (search != pending_rw_commit_c_replies_.end()) {
        Debug("[%lu] Continuing coordinator prepare", transaction_id);

        PendingRWCommitCoordinatorReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();

        std::unordered_set<uint64_t> notify_rws;

        const Transaction &transaction =
            coordinator.GetTransaction(transaction_id);

        const Timestamp prepare_timestamp{
            min_prepare_timestamp_.getTimestamp() + 1, client_id};
        int status = store_.ContinuePrepare(transaction_id, prepare_timestamp,
                                            notify_rws);
        if (status == REPLY_OK) {
            CommitDecision cd = coordinator.ReceivePrepareOK(
                transaction_id, shard_idx_, prepare_timestamp);
            ASSERT(cd.d == Decision::COMMIT);

            reply->commit_timestamp = cd.commit_timestamp;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, transaction, cd.commit_timestamp,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1,
                          std::placeholders::_2, std::placeholders::_3),
                []() {}, COMMIT_TIMEOUT);
        } else if (status == REPLY_FAIL) {
            Debug("[%lu] Continue prepare failed", transaction_id);
            coordinator.Abort(transaction_id);

            const TransportAddress *remote = reply->rid.addr();
            SendRWCommmitCoordinatorReplyFail(*remote, client_id,
                                              client_req_id);
            delete remote;
            delete reply;
            pending_rw_commit_c_replies_.erase(search);

        } else if (status == REPLY_PREPARED) {
            Debug("[%lu] Already prepared", transaction_id);
        } else {  // Keep waiting
            Debug("[%lu] Waiting for conflicting transactions", transaction_id);
        }

        NotifyPendingRWs(notify_rws);
    }
}

void Server::ContinueParticipantPrepare(uint64_t transaction_id) {
    auto search = pending_rw_commit_p_replies_.find(transaction_id);
    if (search != pending_rw_commit_p_replies_.end()) {
        Debug("[%lu] Continuing participant prepare", transaction_id);
        PendingRWCommitParticipantReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();

        std::unordered_set<uint64_t> notify_rws;

        const Timestamp prepare_timestamp{
            min_prepare_timestamp_.getTimestamp() + 1, client_id};

        const int status = store_.ContinuePrepare(
            transaction_id, prepare_timestamp, notify_rws);
        if (status == REPLY_OK) {
            const Transaction &transaction =
                store_.GetPreparedTransaction(transaction_id);

            reply->prepare_timestamp = prepare_timestamp;
            // TODO: Handle timeout
            replica_client_->Prepare(
                transaction_id, transaction, prepare_timestamp,
                std::bind(&Server::PrepareCallback, this, transaction_id,
                          std::placeholders::_1, std::placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);
        } else if (status == REPLY_FAIL) {
            Debug("[%lu] Continue prepare failed", transaction_id);

            // TODO: Handle timeout
            shard_clients_[reply->coordinator_shard]->PrepareAbort(
                transaction_id, shard_idx_,
                std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                          placeholders::_1, placeholders::_2),
                [](int, Timestamp) {}, PREPARE_TIMEOUT);

            // Reply to client
            const TransportAddress *remote = reply->rid.addr();
            SendRWCommmitParticipantReplyFail(*remote, client_id,
                                              client_req_id);
            delete remote;
            delete reply;
            pending_rw_commit_p_replies_.erase(search);
        } else if (status == REPLY_PREPARED) {
            Debug("[%lu] Already prepared", transaction_id);
        } else {  // Keep waiting
            Debug("[%lu] Waiting for conflicting transactions", transaction_id);
        }

        NotifyPendingRWs(notify_rws);
    }
}

void Server::NotifyPendingRWs(const std::unordered_set<uint64_t> &rws) {
    for (uint64_t waiting_rw : rws) {
        ContinueGet(waiting_rw);
        ContinueCoordinatorPrepare(waiting_rw);
        ContinueParticipantPrepare(waiting_rw);
    }
}

void Server::NotifyPendingROs(const std::unordered_set<uint64_t> &ros) {
    for (uint64_t waiting_ro : ros) {
        auto search = pending_ro_commit_replies_.find(waiting_ro);
        Debug("waiting ro: %lu", waiting_ro);
        ASSERT(search != pending_ro_commit_replies_.end());

        PendingROCommitReply *reply = search->second;

        if (NotifyPendingRO(reply)) {
            if (debug_stats_) {
                _Latency_EndRec(&ro_wait_lat_, &reply->wait_lat);
            }
            delete reply;
            pending_ro_commit_replies_.erase(search);
        }
    }
}

bool Server::NotifyPendingRO(PendingROCommitReply *reply) {
    uint64_t n_waiting = reply->n_waiting_prepared;
    n_waiting -= 1;
    bool finish = n_waiting == 0;

    if (finish) {
        Debug("Sending reply for RO transacion: %lu", reply->transaction_id);
        SendROCommitReply(reply);
    }

    reply->n_waiting_prepared = n_waiting;
    return finish;
}

void Server::SendROCommitReply(PendingROCommitReply *reply) {
    uint64_t transaction_id = reply->transaction_id;
    const Timestamp &commit_timestamp = reply->commit_timestamp;

    ro_commit_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    ro_commit_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());
    ro_commit_reply_.set_transaction_id(transaction_id);
    ro_commit_reply_.clear_values();

    int status = REPLY_FAIL;
    std::pair<Timestamp, std::string> value;

    for (auto &k : reply->keys) {
        status = store_.ROGet(transaction_id, k, commit_timestamp, value);
        ASSERT(status == REPLY_OK);
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_val(value.second.c_str());
        value.first.serialize(rreply->mutable_timestamp());
    }

    min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_timestamp);

    const TransportAddress *remote = reply->rid.addr();
    transport_->Timer(0, [this, remote, reply = ro_commit_reply_]() {
        Debug("Sent");
        transport_->SendMessage(this, *remote, reply);
        delete remote;
    });
}

void Server::HandleROCommit(const TransportAddress &remote,
                            proto::ROCommit &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    Debug("Received ROCommit request: %lu", transaction_id);

    std::unordered_set<std::string> keys;
    keys.insert(msg.keys().begin(), msg.keys().end());

    const Timestamp commit_timestamp{msg.commit_timestamp()};
    const Timestamp min_timestamp{msg.min_timestamp()};

    uint64_t n_conflicting_prepared = 0;
    int status = store_.ROBegin(transaction_id, keys, commit_timestamp,
                                min_timestamp, n_conflicting_prepared);
    if (status == REPLY_WAIT) {
        Debug("Waiting for prepared transactions");
        PendingROCommitReply *reply =
            new PendingROCommitReply(client_id, client_req_id, remote.clone());
        reply->transaction_id = transaction_id;
        reply->commit_timestamp = std::move(commit_timestamp);
        reply->n_waiting_prepared = n_conflicting_prepared;
        reply->keys = std::move(keys);
        pending_ro_commit_replies_[transaction_id] = reply;

        if (debug_stats_) {
            _Latency_StartRec(&reply->wait_lat);
        }

        return;
    }

    ro_commit_reply_.mutable_rid()->set_client_id(client_id);
    ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
    ro_commit_reply_.set_transaction_id(transaction_id);
    ro_commit_reply_.clear_values();

    std::pair<Timestamp, std::string> value;

    for (auto &k : keys) {
        status = store_.ROGet(msg.transaction_id(), k,
                              Timestamp(msg.commit_timestamp()), value);
        ASSERT(status == REPLY_OK);
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_val(value.second.c_str());
        value.first.serialize(rreply->mutable_timestamp());
    }

    min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_timestamp);

    transport_->SendMessage(this, remote, ro_commit_reply_);
}

void Server::HandleRWCommitCoordinator(const TransportAddress &remote,
                                       proto::RWCommitCoordinator &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int n_participants = msg.n_participants();

    Transaction transaction{msg.transaction()};
    Timestamp nonblock_timestamp{msg.nonblock_timestamp()};

    std::unordered_set<uint64_t> notify_rws;

    Debug("[%lu] Coordinator for transaction", transaction_id);

    Decision d = coordinator.StartTransaction(client_id, transaction_id,
                                              n_participants, transaction);
    if (d == Decision::TRY_COORD) {
        Debug("[%lu] Trying fast path commit", transaction_id);
        const Timestamp prepare_timestamp{
            min_prepare_timestamp_.getTimestamp() + 1, client_id};
        int status = store_.Prepare(transaction_id, transaction,
                                    prepare_timestamp, nonblock_timestamp);
        Debug("[%lu] store prepare returned status %d", transaction_id, status);
        if (status == REPLY_OK) {
            CommitDecision cd = coordinator.ReceivePrepareOK(
                transaction_id, shard_idx_, prepare_timestamp);
            ASSERT(cd.d == Decision::COMMIT);

            PendingRWCommitCoordinatorReply *pending_reply =
                new PendingRWCommitCoordinatorReply(client_id, client_req_id,
                                                    remote.clone());
            pending_reply->nonblock_timestamp = nonblock_timestamp;
            pending_reply->commit_timestamp = cd.commit_timestamp;
            pending_rw_commit_c_replies_[transaction_id] = pending_reply;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, transaction, cd.commit_timestamp,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1,
                          std::placeholders::_2, std::placeholders::_3),
                []() {}, COMMIT_TIMEOUT);
        } else if (status == REPLY_WAIT) {
            Debug("[%lu] Waiting for conflicting transactions", transaction_id);

            PendingRWCommitCoordinatorReply *pending_reply =
                new PendingRWCommitCoordinatorReply(client_id, client_req_id,
                                                    remote.clone());
            pending_reply->nonblock_timestamp = nonblock_timestamp;
            pending_rw_commit_c_replies_[transaction_id] = pending_reply;
        } else {  // Failed to commit
            Debug("[%lu] Fast path commit failed", transaction_id);
            store_.ReleaseLocks(transaction_id, transaction, notify_rws);
            coordinator.Abort(transaction_id);

            for (uint64_t rw : notify_rws) {
                Debug("[%lu] notify_rw: %lu", transaction_id, rw);
            }

            SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

            NotifyPendingRWs(notify_rws);
        }
    } else if (d == Decision::ABORT) {
        Debug("[%lu] Abort", transaction_id);
        coordinator.Abort(transaction_id);

        SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

    } else {  // Wait for other participants
        Debug("[%lu] Waiting for other participants", transaction_id);

        PendingRWCommitCoordinatorReply *pending_reply =
            new PendingRWCommitCoordinatorReply(client_id, client_req_id,
                                                remote.clone());
        pending_reply->nonblock_timestamp = nonblock_timestamp;
        pending_rw_commit_c_replies_[transaction_id] = pending_reply;
    }
}

void Server::SendRWCommmitCoordinatorReplyOK(
    PendingRWCommitCoordinatorReply *reply, uint64_t response_delay_ms) {
    rw_commit_c_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    rw_commit_c_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());
    rw_commit_c_reply_.set_status(REPLY_OK);
    reply->commit_timestamp.serialize(
        rw_commit_c_reply_.mutable_commit_timestamp());

    Debug("Delaying message by %lu ms", response_delay_ms);
    const TransportAddress *remote = reply->rid.addr();
    transport_->Timer(response_delay_ms,
                      [this, remote, reply = rw_commit_c_reply_]() {
                          Debug("Sent");
                          transport_->SendMessage(this, *remote, reply);
                          delete remote;
                      });
}

void Server::SendRWCommmitCoordinatorReplyFail(const TransportAddress &remote,
                                               uint64_t client_id,
                                               uint64_t client_req_id) {
    rw_commit_c_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_c_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_c_reply_.set_status(REPLY_FAIL);
    rw_commit_c_reply_.clear_commit_timestamp();

    transport_->SendMessage(this, remote, rw_commit_c_reply_);
}

void Server::SendPrepareOKRepliesOK(PendingPrepareOKReply *reply,
                                    Timestamp &commit_timestamp,
                                    uint64_t response_delay_ms) {
    prepare_ok_reply_.set_status(REPLY_OK);
    commit_timestamp.serialize(prepare_ok_reply_.mutable_commit_timestamp());

    for (auto &rid : reply->rids) {
        prepare_ok_reply_.mutable_rid()->set_client_id(rid.client_id());
        prepare_ok_reply_.mutable_rid()->set_client_req_id(rid.client_req_id());

        const TransportAddress *remote = rid.addr();
        Debug("Delaying message by %lu ms", response_delay_ms);
        transport_->Timer(response_delay_ms,
                          [this, remote, reply = prepare_ok_reply_]() {
                              Debug("Sent");
                              transport_->SendMessage(this, *remote, reply);
                              delete remote;
                          });
    }
}

void Server::SendPrepareOKRepliesFail(PendingPrepareOKReply *reply) {
    prepare_ok_reply_.set_status(REPLY_FAIL);
    prepare_ok_reply_.clear_commit_timestamp();

    for (auto &rid : reply->rids) {
        prepare_ok_reply_.mutable_rid()->set_client_id(rid.client_id());
        prepare_ok_reply_.mutable_rid()->set_client_req_id(rid.client_req_id());

        const TransportAddress *remote = rid.addr();
        transport_->Timer(0, [this, remote, reply = prepare_ok_reply_]() {
            Debug("Sent");
            transport_->SendMessage(this, *remote, reply);
            delete remote;
        });
    }
}

void Server::CommitCoordinatorCallback(
    uint64_t transaction_id, transaction_status_t status,
    const std::unordered_set<uint64_t> &notify_rws,
    const std::unordered_set<uint64_t> &notify_ros) {
    ASSERT(status == REPLY_OK);

    PendingRWCommitCoordinatorReply *reply =
        pending_rw_commit_c_replies_[transaction_id];

    Timestamp &commit_timestamp = reply->commit_timestamp;
    Debug("[%lu] COMMIT timestamp: %lu.%lu", transaction_id,
          commit_timestamp.getTimestamp(), commit_timestamp.getID());

    coordinator.Commit(transaction_id);
    uint64_t response_delay_ms = coordinator.CommitWaitMS(commit_timestamp);

    // Reply to client
    SendRWCommmitCoordinatorReplyOK(reply, response_delay_ms);
    delete reply;
    pending_rw_commit_c_replies_.erase(transaction_id);

    // Reply to participants
    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search != pending_prepare_ok_replies_.end()) {
        PendingPrepareOKReply *reply = search->second;

        SendPrepareOKRepliesOK(reply, commit_timestamp, response_delay_ms);

        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);
    }

    NotifyPendingRWs(notify_rws);

    // Reply to waiting RO transactions
    NotifyPendingROs(notify_ros);
}

void Server::SendRWCommmitParticipantReplyOK(
    PendingRWCommitParticipantReply *reply) {
    rw_commit_p_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());
    rw_commit_p_reply_.set_status(REPLY_OK);

    // Respond to client
    const TransportAddress *remote = reply->rid.addr();
    transport_->SendMessage(this, *remote, rw_commit_p_reply_);
    Debug("Sent participant reply");
    delete remote;
}

void Server::SendRWCommmitParticipantReplyFail(const TransportAddress &remote,
                                               uint64_t client_id,
                                               uint64_t client_req_id) {
    rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
    rw_commit_p_reply_.set_status(REPLY_FAIL);

    transport_->SendMessage(this, remote, rw_commit_p_reply_);
}

void Server::HandleRWCommitParticipant(const TransportAddress &remote,
                                       proto::RWCommitParticipant &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int coordinator_shard = msg.coordinator_shard();

    std::unordered_set<uint64_t> notify_rws;

    Debug("[%lu] Participant for transaction", transaction_id);

    Transaction transaction{msg.transaction()};
    Timestamp nonblock_timestamp{msg.nonblock_timestamp()};

    const Timestamp prepare_timestamp{min_prepare_timestamp_.getTimestamp() + 1,
                                      client_id};

    int status = store_.Prepare(transaction_id, transaction, prepare_timestamp,
                                nonblock_timestamp);

    if (status == REPLY_OK) {
        PendingRWCommitParticipantReply *pending_reply =
            new PendingRWCommitParticipantReply(client_id, client_req_id,
                                                remote.clone());
        pending_reply->coordinator_shard = coordinator_shard;
        pending_reply->prepare_timestamp = prepare_timestamp;

        pending_rw_commit_p_replies_[transaction_id] = pending_reply;

        // TODO: Handle timeout
        replica_client_->Prepare(
            transaction_id, transaction, prepare_timestamp,
            std::bind(&Server::PrepareCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);
    } else if (status == REPLY_WAIT) {
        Debug("[%lu] Waiting for conflicting transactions", transaction_id);

        PendingRWCommitParticipantReply *pending_reply =
            new PendingRWCommitParticipantReply(client_id, client_req_id,
                                                remote.clone());
        pending_reply->coordinator_shard = coordinator_shard;
        pending_rw_commit_p_replies_[transaction_id] = pending_reply;
    } else {
        Debug("[%lu] Prepare failed", transaction_id);
        store_.ReleaseLocks(transaction_id, transaction, notify_rws);

        // TODO: Handle timeout
        shard_clients_[coordinator_shard]->PrepareAbort(
            transaction_id, shard_idx_,
            std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                      placeholders::_1, placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);

        // Reply to client
        SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);

        NotifyPendingRWs(notify_rws);
    }
}

void Server::PrepareCallback(uint64_t transaction_id, int status,
                             Timestamp timestamp) {
    ASSERT(status == REPLY_OK);

    PendingRWCommitParticipantReply *reply =
        pending_rw_commit_p_replies_[transaction_id];

    // TODO: Handle timeout
    shard_clients_[reply->coordinator_shard]->PrepareOK(
        transaction_id, shard_idx_, reply->prepare_timestamp,
        std::bind(&Server::PrepareOKCallback, this, transaction_id,
                  placeholders::_1, placeholders::_2),
        [](int, Timestamp) {}, PREPARE_TIMEOUT);

    // Reply to client
    SendRWCommmitParticipantReplyOK(reply);

    delete reply;
    pending_rw_commit_p_replies_.erase(transaction_id);
}

void Server::PrepareOKCallback(uint64_t transaction_id, int status,
                               Timestamp commit_timestamp) {
    Debug("[shard %i] Received PREPARE_OK callback [%d]", shard_idx_, status);

    std::unordered_set<uint64_t> notify_rws;
    std::unordered_set<uint64_t> notify_ros;

    if (status == REPLY_OK) {
        if (store_.Commit(transaction_id, commit_timestamp, notify_rws,
                          notify_ros)) {
            min_prepare_timestamp_ =
                std::max(min_prepare_timestamp_, commit_timestamp);
        }

        // TODO: Handle timeout
        replica_client_->Commit(
            transaction_id, commit_timestamp,
            std::bind(&Server::CommitParticipantCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3),
            []() {}, COMMIT_TIMEOUT);
    } else {
        store_.Abort(transaction_id, notify_rws, notify_ros);

        // TODO: Handle timeout
        replica_client_->Abort(
            transaction_id,
            std::bind(&Server::AbortParticipantCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2),
            []() {}, ABORT_TIMEOUT);
    }

    NotifyPendingROs(notify_ros);
    NotifyPendingRWs(notify_rws);
}

void Server::PrepareAbortCallback(uint64_t transaction_id, int status,
                                  Timestamp timestamp) {
    ASSERT(status == REPLY_OK);

    Debug("[shard %i] Received PREPARE_ABORT callback [%d]", shard_idx_,
          status);
}

void Server::CommitParticipantCallback(
    uint64_t transaction_id, transaction_status_t status,
    const std::unordered_set<uint64_t> &notify_rws,
    const std::unordered_set<uint64_t> &notify_ros) {
    ASSERT(status == REPLY_OK);

    Debug("[shard %i] Received COMMIT participant callback [%d]", shard_idx_,
          status);

    NotifyPendingROs(notify_ros);
    NotifyPendingRWs(notify_rws);
}

void Server::AbortParticipantCallback(
    uint64_t transaction_id, const std::unordered_set<uint64_t> &notify_rws,
    const std::unordered_set<uint64_t> &notify_ros) {
    Debug("[shard %i] Received ABORT participant callback", shard_idx_);

    NotifyPendingROs(notify_ros);
    NotifyPendingRWs(notify_rws);
}

void Server::HandlePrepareOK(const TransportAddress &remote,
                             proto::PrepareOK &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int participant_shard = msg.participant_shard();
    Timestamp prepare_timestamp{msg.prepare_timestamp()};

    std::unordered_set<uint64_t> notify_rws;

    PendingPrepareOKReply *reply = nullptr;
    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search == pending_prepare_ok_replies_.end()) {
        reply =
            new PendingPrepareOKReply(client_id, client_req_id, remote.clone());
        pending_prepare_ok_replies_[transaction_id] = reply;
    } else {
        reply = pending_prepare_ok_replies_[transaction_id];
    }

    // Check for duplicates
    if (reply->rids.count({client_id, client_req_id, nullptr}) == 0) {
        reply->rids.insert({client_id, client_req_id, remote.clone()});
    }

    Debug("[%lu] Received Prepare OK", transaction_id);

    CommitDecision cd = coordinator.ReceivePrepareOK(
        transaction_id, participant_shard, prepare_timestamp);
    if (cd.d == Decision::TRY_COORD) {
        PendingRWCommitCoordinatorReply *coord_reply =
            pending_rw_commit_c_replies_[transaction_id];
        Transaction &transaction = coordinator.GetTransaction(transaction_id);
        Debug("[%lu] Received Prepare OK from all participants",
              transaction_id);
        const Timestamp prepare_timestamp{
            min_prepare_timestamp_.getTimestamp() + 1, client_id};
        int status =
            store_.Prepare(transaction_id, transaction, prepare_timestamp,
                           coord_reply->nonblock_timestamp);
        if (status == REPLY_OK) {
            cd = coordinator.ReceivePrepareOK(transaction_id, shard_idx_,
                                              prepare_timestamp);
            ASSERT(cd.d == Decision::COMMIT);

            coord_reply->commit_timestamp = cd.commit_timestamp;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, transaction, cd.commit_timestamp,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1,
                          std::placeholders::_2, std::placeholders::_3),
                []() {}, COMMIT_TIMEOUT);
        } else if (status == REPLY_FAIL) {  // Failed to commit
            Debug("[%lu] Coordinator prepare failed", transaction_id);
            store_.ReleaseLocks(transaction_id, transaction, notify_rws);
            coordinator.Abort(transaction_id);

            const TransportAddress *addr = coord_reply->rid.addr();
            // Reply to client
            SendRWCommmitCoordinatorReplyFail(*addr,
                                              coord_reply->rid.client_id(),
                                              coord_reply->rid.client_req_id());
            delete addr;
            delete coord_reply;
            pending_rw_commit_c_replies_.erase(transaction_id);

            // Reply to participants
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(transaction_id);

            NotifyPendingRWs(notify_rws);
        } else if (status == REPLY_WAIT) {
            Debug("[%lu] Waiting for conflicting transactions", transaction_id);
        } else {
            NOT_REACHABLE();
        }
    } else if (cd.d == Decision::ABORT) {
        Debug("[%lu] Aborted", transaction_id);
        // Reply to participants
        SendPrepareOKRepliesFail(reply);
        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);
    } else {  // Wait for other participants
        Debug("[%lu] Waiting for other participants", transaction_id);
    }
}

void Server::HandlePrepareAbort(const TransportAddress &remote,
                                proto::PrepareAbort &msg) {
    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    Debug("[%lu] Received Prepare ABORT", transaction_id);

    std::unordered_set<uint64_t> notify_rws;
    if (coordinator.HasTransaction(transaction_id)) {
        store_.ReleaseLocks(transaction_id,
                            coordinator.GetTransaction(transaction_id),
                            notify_rws);
    }

    coordinator.Abort(transaction_id);

    // Reply to client
    auto search = pending_rw_commit_c_replies_.find(transaction_id);
    if (search != pending_rw_commit_c_replies_.end()) {
        PendingRWCommitCoordinatorReply *reply = search->second;

        SendRWCommmitCoordinatorReplyFail(*(reply->rid.addr()),
                                          reply->rid.client_id(),
                                          reply->rid.client_req_id());
        delete reply->rid.addr();
        delete reply;
        pending_rw_commit_c_replies_.erase(search);
    }

    // Reply to other participants
    auto search2 = pending_prepare_ok_replies_.find(transaction_id);
    if (search2 != pending_prepare_ok_replies_.end()) {
        PendingPrepareOKReply *reply = search2->second;
        SendPrepareOKRepliesFail(reply);
        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);
    }

    prepare_abort_reply_.mutable_rid()->set_client_id(client_id);
    prepare_abort_reply_.mutable_rid()->set_client_req_id(client_req_id);
    prepare_abort_reply_.set_status(REPLY_OK);

    transport_->SendMessage(this, remote, prepare_abort_reply_);

    NotifyPendingRWs(notify_rws);
}

void Server::HandleAbort(const TransportAddress &remote, proto::Abort &msg) {
    uint64_t transaction_id = msg.transaction_id();
    const Transaction transaction{msg.transaction()};

    Debug("[%lu] Received Abort request", transaction_id);

    std::unordered_set<uint64_t> notify_rws;
    store_.ReleaseLocks(transaction_id, transaction, notify_rws);

    abort_reply_.mutable_rid()->CopyFrom(msg.rid());
    abort_reply_.set_status(REPLY_OK);

    transport_->SendMessage(this, remote, abort_reply_);

    NotifyPendingRWs(notify_rws);
}

void Server::LeaderUpcall(opnum_t opnum, const string &op, bool &replicate,
                          string &response) {
    Debug("Received LeaderUpcall: %lu %s", opnum, op.c_str());

    Request request;

    request.ParseFromString(op);

    switch (request.op()) {
        case strongstore::proto::Request::PREPARE:
        case strongstore::proto::Request::COMMIT:
        case strongstore::proto::Request::ABORT:
            replicate = true;
            response = op;
            break;
        default:
            Panic("Unrecognized operation.");
    }
}

/* Gets called when a command is issued using client.Invoke(...) to this
 * replica group.
 * opnum is the operation number.
 * op is the request string passed by the client.
 * response is the reply which will be sent back to the client.
 */
void Server::ReplicaUpcall(opnum_t opnum, const string &op, string &response) {
    Debug("Received Upcall: %lu %s", opnum, op.c_str());
    Request request;
    Reply reply;

    request.ParseFromString(op);

    int status = REPLY_OK;
    uint64_t transaction_id = request.txnid();

    if (request.op() == strongstore::proto::Request::PREPARE) {
        store_.Prepare(transaction_id, Transaction(request.prepare().txn()),
                       Timestamp(request.prepare().timestamp()));
    } else if (request.op() == strongstore::proto::Request::COMMIT) {
        Debug("[%lu] Received COMMIT", transaction_id);
        const Timestamp commit_timestamp{request.commit().commit_timestamp()};
        if (request.commit().has_transaction()) {  // Coordinator commit
            store_.Prepare(transaction_id,
                           Transaction(request.commit().transaction()),
                           commit_timestamp);
        }

        uint64_t commit_wait_ms = coordinator.CommitWaitMS(commit_timestamp);

        Debug("[%lu] delaying commit by %lu ms", transaction_id,
              commit_wait_ms);
        transport_->Timer(
            commit_wait_ms, [this, transaction_id, commit_timestamp]() {
                Debug("[%lu] commiting", transaction_id);
                std::unordered_set<uint64_t> notify_rws;
                std::unordered_set<uint64_t> notify_ros;
                if (store_.Commit(transaction_id, commit_timestamp, notify_rws,
                                  notify_ros)) {
                    min_prepare_timestamp_ =
                        std::max(min_prepare_timestamp_, commit_timestamp);
                }

                NotifyPendingRWs(notify_rws);

                // Reply to waiting RO transactions
                NotifyPendingROs(notify_ros);
            });
    } else if (request.op() == strongstore::proto::Request::ABORT) {
        std::unordered_set<uint64_t> notify_rws;
        std::unordered_set<uint64_t> notify_ros;

        Debug("Received ABORT");
        store_.Abort(transaction_id, notify_rws, notify_ros);

        for (uint64_t rw : notify_rws) {
            reply.mutable_notify_rws()->Add(rw);
        }

        for (uint64_t ro : notify_ros) {
            reply.mutable_notify_ros()->Add(ro);
        }
    } else {
        NOT_REACHABLE();
    }

    reply.set_status(status);
    reply.SerializeToString(&response);
}

void Server::UnloggedUpcall(const string &op, string &response) {
    NOT_IMPLEMENTED();
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
    store_.Load(key, value, timestamp);
}

}  // namespace strongstore
