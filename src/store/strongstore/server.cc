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
               const transport::Configuration &replica_config, int shard_idx,
               int replica_idx, Transport *transport,
               InterShardClient &shardClient, const TrueTime &tt,
               bool debug_stats)
    : shard_config_{shard_config},
      replica_config_{replica_config},
      transport_{transport},
      tt_{tt},
      coordinator{tt_},
      shardClient(shardClient),
      timeMaxWrite{0},
      shard_idx_{shard_idx},
      replica_idx_{replica_idx},
      AmLeader{false},
      debug_stats_{debug_stats} {
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);
    store = new strongstore::LockStore();

    if (debug_stats_) {
        _Latency_Init(&prepare_lat_, "prepare_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }

    RegisterHandler(
        &get_, static_cast<MessageServer::MessageHandler>(&Server::HandleGet));
    RegisterHandler(&rw_commit_c_, static_cast<MessageServer::MessageHandler>(
                                       &Server::HandleRWCommitCoordinator));
}

Server::~Server() {
    delete store;
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

    replication::RequestID rid{msg.rid().client_id(),
                               msg.rid().client_req_id()};

    Transaction transcation{msg.transaction()};
    std::unordered_map<uint64_t, int> statuses;

    Debug("Coordinator for transaction: %lu", transaction_id);

    Decision d = coordinator.StartTransaction(rid, transaction_id,
                                              n_participants, transcation);
    if (d == Decision::TRY_COORD) {
        Debug("Trying fast path commit");
        int status = store->Prepare(transaction_id, transcation, statuses);
        if (!status) {
            CommitDecision cd = coordinator.ReceivePrepareOK(
                rid, transaction_id, shard_idx_, timeMaxWrite + 1);
            ASSERT(cd.d == Decision::COMMIT);

            // TODO: Replicate commit

            // request.set_op(proto::Request::COMMIT);
            // request.mutable_commit()->set_timestamp(cd.commitTime);
            // request.mutable_commit()->set_coordinatorshard(shard_idx_);

            rw_commit_c_reply_.mutable_rid()->CopyFrom(msg.rid());
            rw_commit_c_reply_.set_status(REPLY_OK);
            rw_commit_c_reply_.set_commit_timestamp(cd.commitTime);

            if (store->Commit(transaction_id, cd.commitTime, statuses)) {
                timeMaxWrite = std::max(timeMaxWrite, cd.commitTime);
            }

            Debug("COMMIT timestamp: %lu", cd.commitTime);
            coordinator.Commit(transaction_id);
            uint64_t response_delay_ms =
                coordinator.CommitWaitMs(cd.commitTime);

            if (response_delay_ms == 0) {
                Debug("Sent");
                transport_->SendMessage(this, remote, rw_commit_c_reply_);
                // Latency_End(&exec_to_sent_lat_);
            } else {
                Debug("Delaying message by %lu ms", response_delay_ms);
                const TransportAddress *remote2 = remote.clone();
                transport_->Timer(
                    response_delay_ms,
                    [this, remote2, reply = rw_commit_c_reply_]() {
                        Debug("Sent");
                        transport_->SendMessage(this, *remote2, reply);
                        delete remote2;
                        // Latency_End(&exec_to_sent_lat_);
                    });
            }
        } else {  // Failed to commit
            Debug("Fast path commit failed");
            coordinator.Abort(transaction_id);
            // Reply to client
            // replicate = false;
            // reply.set_status(REPLY_FAIL);
            // reply.SerializeToString(&response);
        }
    } else if (d == Decision::ABORT) {
        Debug("Abort: %lu", transaction_id);
        coordinator.Abort(transaction_id);
        // Reply to client
        // replicate = false;
        // reply.set_status(REPLY_FAIL);
        // reply.SerializeToString(&response);
    } else {  // Wait for other participants
        Debug("Waiting for other participants");
        // replicate = false;
        // // Delay response to client
        // response_request_ids.erase(response_request_ids.begin());
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
        case strongstore::proto::Request::PREPARE:
            if (debug_stats_) {
                Latency_Start(&prepare_lat_);
            }
            if (shard_idx_ ==
                request.prepare().coordinatorshard()) {  // I am coordinator

                Debug("Coordinator for transaction: %lu", request.txnid());
                Decision d = coordinator.StartTransaction(
                    requestID, request.txnid(),
                    request.prepare().nparticipants(),
                    Transaction(request.prepare().txn()));
                if (d == Decision::TRY_COORD) {
                    Debug("Trying fast path commit");
                    status = store->Prepare(
                        request.txnid(), Transaction(request.prepare().txn()),
                        statuses);
                    if (!status) {
                        cd = coordinator.ReceivePrepareOK(
                            requestID, request.txnid(), shard_idx_,
                            timeMaxWrite + 1);
                        ASSERT(cd.d == Decision::COMMIT);

                        request.set_op(proto::Request::COMMIT);
                        request.mutable_commit()->set_timestamp(cd.commitTime);
                        request.mutable_commit()->set_coordinatorshard(
                            shard_idx_);

                        replicate = true;
                        request.SerializeToString(&response);
                    } else {  // Failed to commit
                        Debug("Fast path commit failed");
                        coordinator.Abort(request.txnid());
                        // Reply to client
                        replicate = false;
                        reply.set_status(REPLY_FAIL);
                        reply.SerializeToString(&response);
                    }
                } else if (d == Decision::ABORT) {
                    Debug("Abort: %lu", request.txnid());
                    coordinator.Abort(request.txnid());
                    // Reply to client
                    replicate = false;
                    reply.set_status(REPLY_FAIL);
                    reply.SerializeToString(&response);
                } else {  // Wait for other participants
                    Debug("Waiting for other participants");
                    replicate = false;
                    // Delay response to client
                    response_request_ids.erase(response_request_ids.begin());
                }
            } else {  // I am participant
                Debug("Participant for transaction: %lu", request.txnid());
                status = store->Prepare(request.txnid(),
                                        Transaction(request.prepare().txn()),
                                        statuses);
                if (!status) {
                    request.mutable_prepare()->set_timestamp(timeMaxWrite + 1);

                    replicate = true;
                    request.SerializeToString(&response);
                } else {
                    Debug("Prepare failed");
                    // Send message to coordinator
                    shardClient.PrepareAbort(
                        request.prepare().coordinatorshard(), request.txnid(),
                        shard_idx_);
                    // Reply to client
                    replicate = false;
                    reply.set_status(REPLY_FAIL);
                    reply.SerializeToString(&response);
                }
            }
            if (debug_stats_) {
                Latency_End(&prepare_lat_);
            }
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
                    request.mutable_prepare()->set_coordinatorshard(shard_idx_);
                    request.mutable_prepare()->set_nparticipants(
                        coordinator.GetNParticipants(request.txnid()));
                    request.mutable_prepare()->set_timestamp(cd.commitTime);
                    request.mutable_commit()->set_timestamp(cd.commitTime);
                    request.mutable_commit()->set_coordinatorshard(shard_idx_);

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

            if (AmLeader) {
                shardClient.PrepareOK(request.prepare().coordinatorshard(),
                                      request.txnid(), shard_idx_,
                                      request.prepare().timestamp());
            }

            // Reply to client
            break;
        case strongstore::proto::Request::COMMIT:
            if (debug_stats_) {
                Latency_Start(&commit_lat_);
            }
            Debug("Received COMMIT");
            if (request.has_prepare())  // Fast path commit
            {
                store->Prepare(request.txnid(),
                               Transaction(request.prepare().txn()), statuses);
            }
            if (store->Commit(request.txnid(), request.commit().timestamp(),
                              statuses)) {
                timeMaxWrite =
                    std::max(timeMaxWrite, request.commit().timestamp());
            }

            if (shard_idx_ ==
                request.commit().coordinatorshard())  // I am coordinator
            {
                Debug("AmLeader: %d", AmLeader);
                if (AmLeader) {
                    Debug("Replying to client and shards");
                    // Reply to client and shards
                    reply.set_timestamp(request.commit().timestamp());
                    requestIDs = coordinator.GetRequestIDs(request.txnid());
                    response_request_ids.insert(requestIDs.begin(),
                                                requestIDs.end());
                } else {
                    // Don't respond to clients or shards
                    // Only leader needs to respond to client
                    response_request_ids.erase(response_request_ids.begin());
                }

                Debug("COMMIT timestamp: %lu", request.commit().timestamp());
                coordinator.Commit(request.txnid());
                response_delay_ms =
                    coordinator.CommitWaitMs(request.commit().timestamp());
            }
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
