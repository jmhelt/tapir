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

namespace strongstore {

using namespace std;
using namespace proto;
using namespace replication;

Server::Server(const transport::Configuration &shard_config,
               const transport::Configuration &replica_config,
               uint64_t server_id, int shard_idx, int replica_idx,
               Transport *transport, InterShardClient &shardClient,
               const TrueTime &tt, bool debug_stats)
    : shard_config_{shard_config},
      replica_config_{replica_config},
      transport_{transport},
      tt_{tt},
      coordinator{tt_},
      server_id_{server_id},
      shardClient(shardClient),
      timeMaxWrite{0},
      shard_idx_{shard_idx},
      replica_idx_{replica_idx},
      AmLeader{false},
      debug_stats_{debug_stats} {
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);
    store = new strongstore::LockStore();

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
}

Server::~Server() {
    delete store;

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

    get_reply_.mutable_rid()->CopyFrom(msg.rid());
    get_reply_.set_key(msg.key());
    get_reply_.set_val("");

    transport_->SendMessage(this, remote, get_reply_);
}

void Server::HandleRWCommitCoordinator(const TransportAddress &remote,
                                       google::protobuf::Message *m) {
    proto::RWCommitCoordinator &msg =
        *dynamic_cast<proto::RWCommitCoordinator *>(m);

    uint64_t transaction_id = msg.transaction_id();
    int n_participants = msg.n_participants();

    PendingRWCommitCoordinatorReply *pending_reply =
        new PendingRWCommitCoordinatorReply(
            msg.rid().client_id(), msg.rid().client_req_id(), remote.clone());
    pending_rw_commit_c_replies_[transaction_id] = pending_reply;

    replication::RequestID rid{msg.rid().client_id(),
                               msg.rid().client_req_id()};

    Transaction transaction{msg.transaction()};
    std::unordered_map<uint64_t, int> statuses;

    Debug("Coordinator for transaction: %lu", transaction_id);

    Decision d = coordinator.StartTransaction(rid, transaction_id,
                                              n_participants, transaction);
    if (d == Decision::TRY_COORD) {
        Debug("Trying fast path commit");
        int status = store->Prepare(transaction_id, transaction, statuses);
        if (!status) {
            CommitDecision cd = coordinator.ReceivePrepareOK(
                rid, transaction_id, shard_idx_, timeMaxWrite + 1);
            ASSERT(cd.d == Decision::COMMIT);

            pending_reply->commit_timestamp = cd.commitTime;

            // TODO: Handle timeout
            replica_client_->FastPathCommit(
                transaction_id, transaction, cd.commitTime,
                std::bind(&Server::CommitCallback, this, transaction_id,
                          std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);

        } else {  // Failed to commit
            Debug("Fast path commit failed: %lu", transaction_id);
            store->Abort(transaction_id, statuses);
            coordinator.Abort(transaction_id);

            rw_commit_c_reply_.mutable_rid()->CopyFrom(msg.rid());
            rw_commit_c_reply_.set_status(REPLY_FAIL);
            rw_commit_c_reply_.set_commit_timestamp(0);

            transport_->SendMessage(this, remote, rw_commit_c_reply_);

            delete pending_reply->rid.addr();
            delete pending_reply;
            pending_rw_commit_c_replies_.erase(transaction_id);
        }
    } else if (d == Decision::ABORT) {
        Debug("Abort: %lu", transaction_id);
        coordinator.Abort(transaction_id);

        rw_commit_c_reply_.mutable_rid()->CopyFrom(msg.rid());
        rw_commit_c_reply_.set_status(REPLY_FAIL);
        rw_commit_c_reply_.set_commit_timestamp(0);

        transport_->SendMessage(this, remote, rw_commit_c_reply_);

        delete pending_reply->rid.addr();
        delete pending_reply;
        pending_rw_commit_c_replies_.erase(transaction_id);
    } else {  // Wait for other participants
        Debug("Waiting for other participants");
    }
}

void Server::CommitCallback(uint64_t transaction_id,
                            transaction_status_t status) {
    coordinator.Commit(transaction_id);

    PendingRWCommitCoordinatorReply *reply =
        pending_rw_commit_c_replies_[transaction_id];

    uint64_t commit_timestamp = reply->commit_timestamp;
    Debug("COMMIT timestamp: %lu", commit_timestamp);

    rw_commit_c_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    rw_commit_c_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());
    rw_commit_c_reply_.set_status(REPLY_OK);
    rw_commit_c_reply_.set_commit_timestamp(commit_timestamp);

    uint64_t response_delay_ms = coordinator.CommitWaitMs(commit_timestamp);

    const TransportAddress *remote = reply->rid.addr();
    if (response_delay_ms == 0) {
        Debug("Sent");
        transport_->SendMessage(this, *remote, rw_commit_c_reply_);
        delete remote;
    } else {
        Debug("Delaying message by %lu ms", response_delay_ms);
        transport_->Timer(response_delay_ms,
                          [this, remote, reply = rw_commit_c_reply_]() {
                              Debug("Sent");
                              transport_->SendMessage(this, *remote, reply);
                              delete remote;
                          });
    }
    delete reply;
    pending_rw_commit_c_replies_.erase(transaction_id);

    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search != pending_prepare_ok_replies_.end()) {
        PendingPrepareOKReply *reply = search->second;

        prepare_ok_reply_.set_status(REPLY_OK);
        prepare_ok_reply_.set_commit_timestamp(commit_timestamp);

        for (auto &rid : reply->rids) {
            prepare_ok_reply_.mutable_rid()->set_client_id(rid.client_id());
            prepare_ok_reply_.mutable_rid()->set_client_req_id(
                rid.client_req_id());

            const TransportAddress *remote = rid.addr();
            if (response_delay_ms == 0) {
                Debug("Sent");
                transport_->SendMessage(this, *remote, prepare_ok_reply_);
                delete remote;
            } else {
                Debug("Delaying message by %lu ms", response_delay_ms);
                transport_->Timer(response_delay_ms,
                                  [this, remote, reply = prepare_ok_reply_]() {
                                      Debug("Sent");
                                      transport_->SendMessage(this, *remote,
                                                              reply);
                                      delete remote;
                                  });
            }
        }

        delete reply;
        pending_prepare_ok_replies_.erase(transaction_id);
    }
}

void Server::HandleRWCommitParticipant(const TransportAddress &remote,
                                       google::protobuf::Message *m) {
    proto::RWCommitParticipant &msg =
        *dynamic_cast<proto::RWCommitParticipant *>(m);

    uint64_t transaction_id = msg.transaction_id();
    int coordinator_shard = msg.coordinator_shard();

    Debug("Participant for transaction: %lu", transaction_id);

    PendingRWCommitParticipantReply *pending_reply =
        new PendingRWCommitParticipantReply(
            msg.rid().client_id(), msg.rid().client_req_id(), remote.clone());
    pending_rw_commit_p_replies_[transaction_id] = pending_reply;
    pending_reply->coordinator_shard = coordinator_shard;

    replication::RequestID rid{msg.rid().client_id(),
                               msg.rid().client_req_id()};

    Transaction transaction{msg.transaction()};
    std::unordered_map<uint64_t, int> statuses;
    int status = store->Prepare(transaction_id, transaction, statuses);
    if (!status) {
        pending_reply->prepare_timestamp = timeMaxWrite + 1;

        // TODO: Handle timeout
        replica_client_->Prepare(
            transaction_id, transaction,
            std::bind(&Server::PrepareCallback, this, transaction_id,
                      std::placeholders::_1, std::placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);
    } else {
        Debug("Prepare failed");
        // Send message to coordinator
        // shard_clients_[coordinator_shard]->PrepareAbort(
        //     transaction_id, shard_idx_,
        //     std::bind(&Server::PrepareAbortCallback, this, coordinator_shard,
        //               transaction_id, shard_idx_, placeholders::_1,
        //               placeholders::_2));

        // Reply to client
        rw_commit_p_reply_.mutable_rid()->CopyFrom(msg.rid());
        rw_commit_p_reply_.set_status(REPLY_FAIL);

        transport_->SendMessage(this, remote, rw_commit_p_reply_);
    }
}

void Server::PrepareCallback(uint64_t transaction_id, int status,
                             Timestamp timestamp) {
    PendingRWCommitParticipantReply *reply =
        pending_rw_commit_p_replies_[transaction_id];

    rw_commit_p_reply_.mutable_rid()->set_client_id(reply->rid.client_id());
    rw_commit_p_reply_.mutable_rid()->set_client_req_id(
        reply->rid.client_req_id());

    if (status == REPLY_OK) {
        // TODO: Handle timeout
        shard_clients_[reply->coordinator_shard]->PrepareOK(
            transaction_id, shard_idx_, reply->prepare_timestamp,
            std::bind(&Server::PrepareOKCallback, this, transaction_id,
                      placeholders::_1, placeholders::_2),
            [](int, Timestamp) {}, PREPARE_TIMEOUT);
        rw_commit_p_reply_.set_status(REPLY_OK);
    } else {
        rw_commit_p_reply_.set_status(REPLY_FAIL);
    }

    // Respond to client
    const TransportAddress *remote = reply->rid.addr();
    transport_->SendMessage(this, *remote, rw_commit_p_reply_);
    Debug("Sent participant reply");
    pending_rw_commit_p_replies_.erase(transaction_id);
    delete remote;
    delete reply;
}

void Server::PrepareOKCallback(uint64_t transaction_id, int status,
                               Timestamp commit_timestamp) {
    Debug("[shard %i] Received PREPARE_OK callback [%d]", shard_idx_, status);

    switch (status) {
        case REPLY_OK:
            // TODO: Handle timeout
            replica_client_->Commit(
                transaction_id, commit_timestamp.getTimestamp(),
                std::bind(&Server::CommitCallback, this, transaction_id,
                          std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);
            break;
        case REPLY_FAIL:
            // sclient[participantShard]->Abort(
            //     coordShard, txnID, []() {}, []() {}, 1000);
            break;
        default:
            NOT_REACHABLE();
    }
}

void Server::HandlePrepareOK(const TransportAddress &remote,
                             google::protobuf::Message *m) {
    proto::PrepareOK &msg = *dynamic_cast<proto::PrepareOK *>(m);

    uint64_t client_id = msg.rid().client_id();
    uint64_t client_req_id = msg.rid().client_req_id();

    uint64_t transaction_id = msg.transaction_id();
    int participant_shard = msg.participant_shard();
    uint64_t prepare_timestamp = msg.prepare_timestamp();

    PendingPrepareOKReply *pending_reply = nullptr;
    auto search = pending_prepare_ok_replies_.find(transaction_id);
    if (search == pending_prepare_ok_replies_.end()) {
        pending_reply =
            new PendingPrepareOKReply(client_id, client_req_id, remote.clone());
        pending_prepare_ok_replies_[transaction_id] = pending_reply;
    } else {
        pending_reply = pending_prepare_ok_replies_[transaction_id];
    }

    // Check for duplicates
    if (pending_reply->rids.count({client_id, client_req_id, nullptr}) == 0) {
        pending_reply->rids.insert({client_id, client_req_id, remote.clone()});
    }

    Debug("Received Prepare OK: %lu", transaction_id);

    replication::RequestID rid{client_id, client_req_id};

    std::unordered_map<uint64_t, int> statuses;

    CommitDecision cd = coordinator.ReceivePrepareOK(
        rid, transaction_id, participant_shard, prepare_timestamp);
    if (cd.d == Decision::TRY_COORD) {
        PendingRWCommitCoordinatorReply *pending_reply =
            pending_rw_commit_c_replies_[transaction_id];
        Transaction transaction = coordinator.GetTransaction(transaction_id);
        Debug("Received Prepare OK from all participants");
        int status = store->Prepare(transaction_id, transaction, statuses);
        if (!status) {
            cd = coordinator.ReceivePrepareOK(rid, transaction_id, shard_idx_,
                                              timeMaxWrite + 1);
            ASSERT(cd.d == Decision::COMMIT);

            pending_reply->commit_timestamp = cd.commitTime;

            // TODO: Handle timeout
            replica_client_->FastPathCommit(
                transaction_id, transaction, cd.commitTime,
                std::bind(&Server::CommitCallback, this, transaction_id,
                          std::placeholders::_1),
                []() {}, COMMIT_TIMEOUT);
        } else {  // Failed to commit
            Debug("Coordinator prepare failed: %lu", transaction_id);
            store->Abort(transaction_id, statuses);
            coordinator.Abort(transaction_id);

            rw_commit_c_reply_.mutable_rid()->CopyFrom(msg.rid());
            rw_commit_c_reply_.set_status(REPLY_FAIL);
            rw_commit_c_reply_.set_commit_timestamp(0);

            transport_->SendMessage(this, remote, rw_commit_c_reply_);

            delete pending_reply->rid.addr();
            delete pending_reply;
            pending_rw_commit_c_replies_.erase(transaction_id);
        }
    } else if (cd.d == Decision::ABORT) {
        Debug("Abort: %lu", transaction_id);
        coordinator.Abort(transaction_id);

        rw_commit_c_reply_.mutable_rid()->CopyFrom(msg.rid());
        rw_commit_c_reply_.set_status(REPLY_FAIL);
        rw_commit_c_reply_.set_commit_timestamp(0);

        transport_->SendMessage(this, remote, rw_commit_c_reply_);

        auto search = pending_rw_commit_c_replies_.find(transaction_id);
        if (search != pending_rw_commit_c_replies_.end()) {
            delete search->second->rid.addr();
            delete search->second;
            pending_rw_commit_c_replies_.erase(search);
        }
    } else {  // Wait for other participants
        Debug("Waiting for other participants");
    }
}

void Server::LeaderUpcall(
    opnum_t opnum, const string &op, bool &replicate, string &response,
    std::unordered_set<replication::RequestID> &response_request_ids) {
    Debug("Received LeaderUpcall: %lu %s", opnum, op.c_str());

    Request request;
    Reply reply;
    int status;
    CommitDecision cd;
    replication::RequestID requestID = *response_request_ids.begin();
    std::unordered_set<replication::RequestID> requestIDs;
    std::unordered_map<uint64_t, int> statuses;

    request.ParseFromString(op);

    switch (request.op()) {
        case strongstore::proto::Request::GET:
            if (request.get().has_timestamp()) {
                pair<Timestamp, string> val;
                status = store->Get(request.txnid(), request.get().key(),
                                    request.get().timestamp(), val, statuses);
                if (status == 0) {
                    reply.set_value(val.second);
                }
            } else {
                pair<Timestamp, string> val;
                status = store->Get(request.txnid(), request.get().key(), val);
                if (status == 0) {
                    reply.set_value(val.second);
                    reply.set_timestamp(val.first.getTimestamp());
                }
            }
            replicate = false;
            reply.set_status(status);
            reply.SerializeToString(&response);
            break;
        case strongstore::proto::Request::PREPARE_OK:  // Only coordinator
                                                       // receives prepare OK
            Debug("Received Prepare OK");
            cd = coordinator.ReceivePrepareOK(
                requestID, request.txnid(),
                request.prepareok().participantshard(),
                request.prepareok().timestamp());
            if (cd.d == Decision::TRY_COORD) {
                Debug("Received Prepare OK from all participants");
                status = store->Prepare(
                    request.txnid(),
                    coordinator.GetTransaction(request.txnid()), statuses);
                if (!status) {
                    cd = coordinator.ReceivePrepareOK(
                        requestID, request.txnid(), shard_idx_,
                        timeMaxWrite + 1);
                    ASSERT(cd.d == Decision::COMMIT);

                    request.clear_prepareok();
                    request.set_op(proto::Request::COMMIT);
                    coordinator.GetTransaction(request.txnid())
                        .serialize(request.mutable_prepare()->mutable_txn());
                    // request.mutable_prepare()->set_coordinatorshard(shard_idx_);
                    // request.mutable_prepare()->set_nparticipants(
                    // coordinator.GetNParticipants(request.txnid()));
                    request.mutable_prepare()->set_timestamp(cd.commitTime);
                    request.mutable_commit()->set_timestamp(cd.commitTime);
                    // request.mutable_commit()->set_coordinatorshard(shard_idx_);

                    replicate = true;
                    request.SerializeToString(&response);
                } else {  // Failed to commit
                    Debug("Fast path commit failed");
                    requestIDs = coordinator.GetRequestIDs(request.txnid());
                    coordinator.Abort(request.txnid());
                    response_request_ids.insert(requestIDs.begin(),
                                                requestIDs.end());
                    replicate = false;
                    reply.set_status(REPLY_FAIL);
                    reply.SerializeToString(&response);
                    // if (debug_stats_) {
                    //     Latency_End(&prepare_lat_);
                    // }
                }
            } else if (cd.d == Decision::ABORT) {
                Debug("Abort: %lu", request.txnid());
                requestIDs = coordinator.GetRequestIDs(request.txnid());
                coordinator.Abort(request.txnid());
                response_request_ids.insert(requestIDs.begin(),
                                            requestIDs.end());
                // Reply to client and shards
                replicate = false;
                reply.set_status(REPLY_FAIL);
                reply.SerializeToString(&response);
                // if (debug_stats_) {
                //     Latency_End(&prepare_lat_);
                // }
            } else {  // Wait for other participants
                Debug("Waiting for other participants");
                replicate = false;
                // Delay response to client
                response_request_ids.erase(response_request_ids.begin());
            }
            break;
        case strongstore::proto::Request::
            PREPARE_ABORT:  // Only coordinator receives prepare ABORT
            Debug("Received Prepare ABORT");
            requestIDs = coordinator.GetRequestIDs(request.txnid());
            coordinator.Abort(request.txnid());
            response_request_ids.insert(requestIDs.begin(), requestIDs.end());
            replicate = false;
            reply.set_status(REPLY_FAIL);
            reply.SerializeToString(&response);
            break;
        case strongstore::proto::Request::PREPARE:
            replicate = true;
            response = op;
            break;
        case strongstore::proto::Request::COMMIT:
            replicate = true;
            response = op;
            break;
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
void Server::ReplicaUpcall(
    opnum_t opnum, const string &op, string &response,
    std::unordered_set<replication::RequestID> &response_request_ids,
    uint64_t &response_delay_ms) {
    Debug("Received Upcall: %lu %s", opnum, op.c_str());
    Request request;
    Reply reply;
    std::unordered_set<replication::RequestID> requestIDs;
    int status = REPLY_OK;

    request.ParseFromString(op);

    std::unordered_map<uint64_t, int> statuses;
    switch (request.op()) {
        case strongstore::proto::Request::PREPARE:
            // get a prepare timestamp and return to client
            store->Prepare(request.txnid(),
                           Transaction(request.prepare().txn()), statuses);

            // if (AmLeader) {
            //     shardClient.PrepareOK(request.prepare().coordinatorshard(),
            //                           request.txnid(), shard_idx_,
            //                           request.prepare().timestamp());
            // }
            break;
        case strongstore::proto::Request::COMMIT:
            if (debug_stats_) {
                Latency_Start(&commit_lat_);
            }
            Debug("Received COMMIT");
            if (request.commit().has_transaction()) {  // Fast path commit
                store->Prepare(request.txnid(),
                               Transaction(request.commit().transaction()),
                               statuses);
            }
            if (store->Commit(request.txnid(), request.commit().timestamp(),
                              statuses)) {
                timeMaxWrite =
                    std::max(timeMaxWrite, request.commit().timestamp());
            }

            // if (shard_idx_ ==
            //     request.commit().coordinatorshard())  // I am coordinator
            // {
            //     Debug("AmLeader: %d", AmLeader);
            //     if (AmLeader) {
            //         Debug("Replying to client and shards");
            //         // Reply to client and shards
            //         reply.set_timestamp(request.commit().timestamp());
            //         requestIDs = coordinator.GetRequestIDs(request.txnid());
            //         response_request_ids.insert(requestIDs.begin(),
            //                                     requestIDs.end());
            //     } else {
            //         // Don't respond to clients or shards
            //         // Only leader needs to respond to client
            //         response_request_ids.erase(response_request_ids.begin());
            //     }

            //     Debug("COMMIT timestamp: %lu", request.commit().timestamp());
            //     coordinator.Commit(request.txnid());
            //     response_delay_ms =
            //         coordinator.CommitWaitMs(request.commit().timestamp());
            // }
            if (debug_stats_) {
                Latency_End(&commit_lat_);
            }
            break;
        case strongstore::proto::Request::ABORT:
            Debug("Received ABORT");
            store->Abort(request.txnid(), statuses);
            break;
        default:
            Panic("Unrecognized operation.");
    }
    reply.set_status(status);
    reply.SerializeToString(&response);
}

void Server::UnloggedUpcall(const string &op, string &response) {
    Request request;
    Reply reply;
    int status;

    request.ParseFromString(op);

    ASSERT(request.op() == strongstore::proto::Request::GET);

    if (request.get().has_timestamp()) {
        std::unordered_map<uint64_t, int> statuses;

        pair<Timestamp, string> val;
        status = store->Get(request.txnid(), request.get().key(),
                            request.get().timestamp(), val, statuses);
        if (status == 0) {
            reply.set_value(val.second);
        }
    } else {
        pair<Timestamp, string> val;
        status = store->Get(request.txnid(), request.get().key(), val);
        if (status == 0) {
            reply.set_value(val.second);
            reply.set_timestamp(val.first.getTimestamp());
        }
    }

    reply.set_status(status);
    reply.SerializeToString(&response);
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
    store->Load(key, value, timestamp);
}

}  // namespace strongstore
