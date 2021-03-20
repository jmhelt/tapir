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
    : shard_config_{shard_config},
      replica_config_{replica_config},
      transport_{transport},
      tt_{tt},
      coordinator{tt_},
      server_id_{server_id},
      max_write_timestamp_{},
      shard_idx_{shard_idx},
      replica_idx_{replica_idx},
      consistency_{consistency},
      debug_stats_{debug_stats} {
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);
    store_ = new strongstore::LockStore();

    for (int i = 0; i < shard_config_.n; i++) {
        shard_clients_.push_back(
            new ShardClient(shard_config_, transport, server_id_, i));
    }

    replica_client_ =
        new ReplicaClient(replica_config_, transport_, server_id_, shard_idx_);

    if (debug_stats_) {
        _Latency_Init(&prepare_lat_, "prepare_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }

    RegisterHandler(
        &get_, static_cast<MessageServer::MessageHandler>(&Server::HandleGet));
    RegisterHandler(&rw_commit_c_, static_cast<MessageServer::MessageHandler>(
                                       &Server::HandleRWCommitCoordinator));
    RegisterHandler(&rw_commit_p_, static_cast<MessageServer::MessageHandler>(
                                       &Server::HandleRWCommitParticipant));

    RegisterHandler(&prepare_ok_, static_cast<MessageServer::MessageHandler>(
                                      &Server::HandlePrepareOK));
    RegisterHandler(&prepare_abort_, static_cast<MessageServer::MessageHandler>(
                                         &Server::HandlePrepareAbort));

    RegisterHandler(&ro_commit_, static_cast<MessageServer::MessageHandler>(
                                     &Server::HandleROCommit));
}

Server::~Server() {
    delete store_;

    for (auto s : shard_clients_) {
        delete s;
    }

    delete replica_client_;

    if (debug_stats_) {
        Latency_Dump(&prepare_lat_);
        Latency_Dump(&commit_lat_);
    }
}

void Server::HandleGet(const TransportAddress &remote,
                       google::protobuf::Message *m) {
    proto::Get &msg = *dynamic_cast<proto::Get *>(m);

    Debug("Received GET request: %s", msg.key().c_str());

    std::pair<Timestamp, std::string> value;
    store_->Get(msg.transaction_id(), msg.key(), value);

    get_reply_.mutable_rid()->CopyFrom(msg.rid());
    get_reply_.set_key(msg.key());
    value.first.serialize(get_reply_.mutable_timestamp());
    get_reply_.set_val(value.second);

    transport_->SendMessage(this, remote, get_reply_);
}

void Server::NotifyPendingROs(const std::unordered_set<uint64_t> &ros) {
    for (uint64_t waiting_ro : ros) {
        auto search = pending_ro_commit_repies_.find(waiting_ro);
        ASSERT(search != pending_ro_commit_repies_.end());

        PendingROCommitReply *reply = search->second;

        if (NotifyPendingRO(reply)) {
            delete reply;
            pending_ro_commit_repies_.erase(search);
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
    Timestamp commit_timestamp = Timestamp(reply->commit_timestamp);

    ro_commit_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    ro_commit_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());
    ro_commit_reply_.set_transaction_id(transaction_id);
    ro_commit_reply_.clear_values();

    int status = REPLY_FAIL;
    std::pair<Timestamp, std::string> value;

    for (auto &k : reply->keys) {
        status = store_->ROGet(transaction_id, k, commit_timestamp, value);
        ASSERT(status == REPLY_OK);
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_val(value.second.c_str());
        value.first.serialize(rreply->mutable_timestamp());
    }

    const TransportAddress *remote = reply->rid.addr();
    transport_->Timer(0, [this, remote, reply = ro_commit_reply_]() {
        Debug("Sent");
        transport_->SendMessage(this, *remote, reply);
        delete remote;
    });
}

void Server::HandleROCommit(const TransportAddress &remote,
                            google::protobuf::Message *m) {
    proto::ROCommit &msg = *dynamic_cast<proto::ROCommit *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

    Debug("Received ROCommit request: %lu", transaction_id);

    std::unordered_set<std::string> keys;
    keys.insert(msg.keys().begin(), msg.keys().end());

    std::unordered_set<uint64_t> conflicting_prepares;

    uint64_t n_conflicting_prepared = 0;
    if (store_->ROBegin(transaction_id, keys, n_conflicting_prepared)) {
        Debug("Waiting for prepared transactions");
        PendingROCommitReply *reply =
            new PendingROCommitReply(client_id, client_req_id, remote.clone());
        reply->transaction_id = transaction_id;
        reply->commit_timestamp = msg.commit_timestamp();
        reply->n_waiting_prepared = n_conflicting_prepared;
        reply->keys = std::move(keys);
        pending_ro_commit_repies_[transaction_id] = reply;

        return;
    }

    ro_commit_reply_.mutable_rid()->set_client_id(client_id);
    ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
    ro_commit_reply_.set_transaction_id(transaction_id);
    ro_commit_reply_.clear_values();

    int status = REPLY_FAIL;
    std::pair<Timestamp, std::string> value;

    for (auto &k : keys) {
        status = store_->ROGet(msg.transaction_id(), k,
                               Timestamp(msg.commit_timestamp()), value);
        ASSERT(status == REPLY_OK);
        proto::ReadReply *rreply = ro_commit_reply_.add_values();
        rreply->set_val(value.second.c_str());
        value.first.serialize(rreply->mutable_timestamp());
    }

    transport_->SendMessage(this, remote, ro_commit_reply_);
}

void Server::HandleRWCommitCoordinator(const TransportAddress &remote,
                                       google::protobuf::Message *m) {
    proto::RWCommitCoordinator &msg =
        *dynamic_cast<proto::RWCommitCoordinator *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int n_participants = msg.n_participants();

    Transaction transaction{msg.transaction()};
    Timestamp nonblock_timestamp{msg.nonblock_timestamp()};

    Debug("Coordinator for transaction: %lu", transaction_id);

    Decision d = coordinator.StartTransaction(client_id, transaction_id,
                                              n_participants, transaction);
    if (d == Decision::TRY_COORD) {
        Debug("Trying fast path commit");
        int status = REPLY_FAIL;
        if (consistency_ == Consistency::SS) {
            status = store_->Prepare(transaction_id, transaction);
        } else if (consistency_ == Consistency::RSS) {
            status = store_->Prepare(transaction_id, transaction,
                                     nonblock_timestamp);
        } else {
            NOT_REACHABLE();
        }
        if (status == REPLY_OK) {
            Timestamp prepare_timestamp{max_write_timestamp_.getTimestamp() + 1,
                                        client_id};
            CommitDecision cd = coordinator.ReceivePrepareOK(
                transaction_id, shard_idx_, prepare_timestamp);
            ASSERT(cd.d == Decision::COMMIT);

            PendingRWCommitCoordinatorReply *pending_reply =
                new PendingRWCommitCoordinatorReply(client_id, client_req_id,
                                                    remote.clone());
            pending_reply->commit_timestamp = cd.commit_timestamp;
            pending_rw_commit_c_replies_[transaction_id] = pending_reply;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, transaction, cd.commit_timestamp,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1,
                          std::placeholders::_2),
                []() {}, COMMIT_TIMEOUT);
        } else {  // Failed to commit
            Debug("Fast path commit failed: %lu", transaction_id);
            store_->Abort(transaction_id);
            coordinator.Abort(transaction_id);

            SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);
        }
    } else if (d == Decision::ABORT) {
        Debug("Abort: %lu", transaction_id);
        coordinator.Abort(transaction_id);

        SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

    } else {  // Wait for other participants
        Debug("Waiting for other participants");

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
    const std::unordered_set<uint64_t> &notify_ros) {
    ASSERT(status == REPLY_OK);

    PendingRWCommitCoordinatorReply *reply =
        pending_rw_commit_c_replies_[transaction_id];

    Timestamp &commit_timestamp = reply->commit_timestamp;
    Debug("COMMIT timestamp: %lu %lu", commit_timestamp.getTimestamp(),
          commit_timestamp.getID());

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

    // Reply to waiting RO transactions
    std::unordered_set<uint64_t> ros{};
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
                                       google::protobuf::Message *m) {
    proto::RWCommitParticipant &msg =
        *dynamic_cast<proto::RWCommitParticipant *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int coordinator_shard = msg.coordinator_shard();

    Debug("Participant for transaction: %lu", transaction_id);

    Transaction transaction{msg.transaction()};

    if (!store_->Prepare(transaction_id, transaction)) {
        PendingRWCommitParticipantReply *pending_reply =
            new PendingRWCommitParticipantReply(client_id, client_req_id,
                                                remote.clone());
        pending_reply->coordinator_shard = coordinator_shard;
        pending_reply->prepare_timestamp = {
            max_write_timestamp_.getTimestamp() + 1, client_id};

        pending_rw_commit_p_replies_[transaction_id] = pending_reply;

        // TODO: Handle timeout
        replica_client_->Prepare(
            transaction_id, transaction,
            std::bind(&Server::PrepareCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);
    } else {
        Debug("Prepare failed");
        store_->Abort(transaction_id);
        // TODO: Handle timeout
        shard_clients_[coordinator_shard]->PrepareAbort(
            transaction_id, shard_idx_,
            std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                      placeholders::_1, placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);

        // Reply to client
        SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);
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

    if (status == REPLY_OK) {
        PendingRWCommitParticipantReply *reply =
            pending_rw_commit_p_replies_[transaction_id];

        commit_timestamp.setID(reply->rid.client_id());

        // TODO: Handle timeout
        replica_client_->Commit(
            transaction_id, commit_timestamp,
            std::bind(&Server::CommitParticipantCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2),
            []() {}, COMMIT_TIMEOUT);
    } else {
        // TODO: Handle timeout
        replica_client_->Abort(
            transaction_id,
            std::bind(&Server::AbortParticipantCallback, this, transaction_id,
                      std::placeholders::_1),
            []() {}, ABORT_TIMEOUT);
    }
}

void Server::PrepareAbortCallback(uint64_t transaction_id, int status,
                                  Timestamp timestamp) {
    ASSERT(status == REPLY_OK);

    Debug("[shard %i] Received PREPARE_ABORT callback [%d]", shard_idx_,
          status);
}

void Server::CommitParticipantCallback(
    uint64_t transaction_id, transaction_status_t status,
    const std::unordered_set<uint64_t> &notify_ros) {
    ASSERT(status == REPLY_OK);

    Debug("[shard %i] Received COMMIT participant callback [%d]", shard_idx_,
          status);

    NotifyPendingROs(notify_ros);
}

void Server::AbortParticipantCallback(
    uint64_t transaction_id, const std::unordered_set<uint64_t> &notify_ros) {
    Debug("[shard %i] Received ABORT participant callback", shard_idx_);

    NotifyPendingROs(notify_ros);
}

void Server::HandlePrepareOK(const TransportAddress &remote,
                             google::protobuf::Message *m) {
    proto::PrepareOK &msg = *dynamic_cast<proto::PrepareOK *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int participant_shard = msg.participant_shard();
    Timestamp prepare_timestamp{msg.prepare_timestamp()};

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

    Debug("Received Prepare OK: %lu", transaction_id);

    CommitDecision cd = coordinator.ReceivePrepareOK(
        transaction_id, participant_shard, prepare_timestamp);
    if (cd.d == Decision::TRY_COORD) {
        PendingRWCommitCoordinatorReply *coord_reply =
            pending_rw_commit_c_replies_[transaction_id];
        Transaction &transaction = coordinator.GetTransaction(transaction_id);
        Debug("Received Prepare OK from all participants");
        int status = REPLY_FAIL;
        if (consistency_ == Consistency::SS) {
            status = store_->Prepare(transaction_id, transaction);
        } else if (consistency_ == Consistency::RSS) {
            status = store_->Prepare(transaction_id, transaction,
                                     coord_reply->nonblock_timestamp);
        } else {
            NOT_REACHABLE();
        }
        if (status == REPLY_OK) {
            Timestamp prepare_timestamp{max_write_timestamp_.getTimestamp() + 1,
                                        client_id};
            cd = coordinator.ReceivePrepareOK(transaction_id, shard_idx_,
                                              prepare_timestamp);
            ASSERT(cd.d == Decision::COMMIT);

            coord_reply->commit_timestamp = cd.commit_timestamp;

            // TODO: Handle timeout
            replica_client_->CoordinatorCommit(
                transaction_id, transaction, cd.commit_timestamp,
                std::bind(&Server::CommitCoordinatorCallback, this,
                          transaction_id, std::placeholders::_1,
                          std::placeholders::_2),
                []() {}, COMMIT_TIMEOUT);
        } else {  // Failed to commit
            Debug("Coordinator prepare failed: %lu", transaction_id);
            store_->Abort(transaction_id);
            coordinator.Abort(transaction_id);

            // Reply to client
            SendRWCommmitCoordinatorReplyFail(*(coord_reply->rid.addr()),
                                              coord_reply->rid.client_id(),
                                              coord_reply->rid.client_req_id());
            delete coord_reply->rid.addr();
            delete coord_reply;
            pending_rw_commit_c_replies_.erase(transaction_id);

            // Reply to participants
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(transaction_id);
        }
    } else if (cd.d == Decision::ABORT) {
        Debug("Aborted: %lu", transaction_id);
        // Reply to participants
        SendPrepareOKRepliesFail(reply);
        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);
    } else {  // Wait for other participants
        Debug("Waiting for other participants");
    }
}

void Server::HandlePrepareAbort(const TransportAddress &remote,
                                google::protobuf::Message *m) {
    Debug("Received Prepare ABORT");
    proto::PrepareAbort &msg = *dynamic_cast<proto::PrepareAbort *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();
    uint64_t transaction_id = msg.transaction_id();

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
    int status = REPLY_OK;
    std::unordered_set<uint64_t> notify_ros;
    Timestamp commit_timestamp;

    request.ParseFromString(op);

    switch (request.op()) {
        case strongstore::proto::Request::PREPARE:
            store_->Prepare(request.txnid(),
                            Transaction(request.prepare().txn()));
            break;
        case strongstore::proto::Request::COMMIT:
            if (debug_stats_) {
                Latency_Start(&commit_lat_);
            }
            Debug("Received COMMIT");
            if (request.commit().has_transaction()) {  // Coordinator commit
                store_->Prepare(request.txnid(),
                                Transaction(request.commit().transaction()));
            }
            commit_timestamp = {request.commit().commit_timestamp()};
            if (store_->Commit(request.txnid(), commit_timestamp, notify_ros)) {
                max_write_timestamp_ =
                    std::max(max_write_timestamp_, commit_timestamp);
            }
            if (debug_stats_) {
                Latency_End(&commit_lat_);
            }

            for (uint64_t ro : notify_ros) {
              reply.mutable_notify_ros()->Add(ro);
            }
            break;
        case strongstore::proto::Request::ABORT:
            Debug("Received ABORT");
            store_->Abort(request.txnid());
            break;
        default:
            Panic("Unrecognized operation.");
    }
    reply.set_status(status);
    reply.SerializeToString(&response);
}

void Server::UnloggedUpcall(const string &op, string &response) {
    NOT_IMPLEMENTED();
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
    store_->Load(key, value, timestamp);
}

}  // namespace strongstore
