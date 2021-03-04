// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/server.cc:
 *   Implementation of a single transactional key-value server with strong consistency.
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

namespace strongstore
{

    using namespace std;
    using namespace proto;
    using namespace replication;

    Server::Server(InterShardClient &shardClient, int groupIdx, int myIdx,
                   Mode mode, uint64_t skew, uint64_t error) : timeServer{skew, error}, coordinator{timeServer},
                                                               shardClient(shardClient), timeMaxWrite{0},
                                                               groupIdx(groupIdx), myIdx(myIdx), mode(mode), AmLeader{false}
    {
        timeServer = TrueTime(skew, error);

        switch (mode)
        {
        case MODE_LOCK:
        case MODE_SPAN_LOCK:
            store = new strongstore::LockStore();
            break;
        case MODE_OCC:
        case MODE_SPAN_OCC:
            store = new strongstore::OCCStore();
            break;
        default:
            NOT_REACHABLE();
        }
    }

    Server::~Server()
    {
        delete store;
    }

    void
    Server::LeaderUpcall(opnum_t opnum, const string &str1, bool &replicate, string &str2, std::unordered_set<replication::RequestID> &resRequestIDs)
    {
        Debug("Received LeaderUpcall: %lu %s", opnum, str1.c_str());

        Request request;
        Reply reply;
        int status;
        CommitDecision cd;
        replication::RequestID requestID = *resRequestIDs.begin();
        std::unordered_set<replication::RequestID> requestIDs;

        request.ParseFromString(str1);

        switch (request.op())
        {
        case strongstore::proto::Request::GET:
            if (request.get().has_timestamp())
            {
                pair<Timestamp, string> val;
                status = store->Get(request.txnid(), request.get().key(),
                                    request.get().timestamp(), val);
                if (status == 0)
                {
                    reply.set_value(val.second);
                }
            }
            else
            {
                pair<Timestamp, string> val;
                status = store->Get(request.txnid(), request.get().key(), val);
                if (status == 0)
                {
                    reply.set_value(val.second);
                    reply.set_timestamp(val.first.getTimestamp());
                }
            }
            replicate = false;
            reply.set_status(status);
            reply.SerializeToString(&str2);
            break;
        case strongstore::proto::Request::PREPARE:

            if (groupIdx == request.prepare().coordinatorshard()) // I am coordinator
            {
                Debug("Coordinator for transaction: %lu", request.txnid());
                Decision d = coordinator.StartTransaction(requestID, request.txnid(), request.prepare().nparticipants(), Transaction(request.prepare().txn()));
                if (d == Decision::TRY_COORD)
                {
                    Debug("Trying fast path commit");
                    status = store->Prepare(request.txnid(), Transaction(request.prepare().txn()));
                    if (!status)
                    {
                        cd = coordinator.ReceivePrepareOK(requestID, request.txnid(), groupIdx, timeMaxWrite + 1);
                        ASSERT(cd.d == Decision::COMMIT);

                        request.set_op(proto::Request::COMMIT);
                        request.mutable_commit()->set_timestamp(cd.commitTime);

                        replicate = true;
                        request.SerializeToString(&str2);
                    }
                    else // Failed to commit
                    {
                        Debug("Fast path commit failed");
                        coordinator.Abort(request.txnid());
                        // Reply to client
                        replicate = false;
                        reply.set_status(status);
                        reply.SerializeToString(&str2);
                    }
                }
                else // Wait for other participants
                {
                    Debug("Waiting for other participants");
                    replicate = false;
                    // Delay response to client
                    resRequestIDs.erase(resRequestIDs.begin());
                }
            }
            else // I am participant
            {
                Debug("Participant for transaction: %lu", request.txnid());
                status = store->Prepare(request.txnid(), Transaction(request.prepare().txn()));
                if (!status)
                {
                    request.mutable_prepare()->set_timestamp(timeMaxWrite + 1);

                    replicate = true;
                    request.SerializeToString(&str2);
                }
                else
                {
                    Debug("Prepare failed");
                    store->Abort(request.txnid(), Transaction(request.prepare().txn()));
                    // Send message to coordinator
                    shardClient.PrepareAbort(request.prepare().coordinatorshard(), request.txnid());
                    // Reply to client
                    replicate = false;
                    reply.set_status(status);
                    reply.SerializeToString(&str2);
                }
            }
            break;
        case strongstore::proto::Request::PREPARE_OK:
            Debug("Received Prepare OK");
            cd = coordinator.ReceivePrepareOK(requestID, request.txnid(), request.prepareok().participantshard(), request.prepareok().timestamp());
            if (cd.d == Decision::TRY_COORD)
            {
                Debug("Received Prepare OK from all participants");
                status = store->Prepare(request.txnid(), coordinator.GetTransaction(request.txnid()));
                if (!status)
                {
                    cd = coordinator.ReceivePrepareOK(requestID, request.txnid(), groupIdx, timeMaxWrite + 1);
                    ASSERT(cd.d == Decision::COMMIT);

                    request.clear_prepareok();
                    request.set_op(proto::Request::COMMIT);
                    request.mutable_commit()->set_timestamp(cd.commitTime);

                    replicate = true;
                    request.SerializeToString(&str2);
                }
                else // Failed to commit
                {
                    Debug("Fast path commit failed");
                    coordinator.Abort(request.txnid());
                    requestIDs = coordinator.GetRequestIDs(request.txnid());
                    resRequestIDs.insert(requestIDs.begin(), requestIDs.end());
                    replicate = false;
                    reply.set_status(status);
                    reply.SerializeToString(&str2);
                }
            }
            else // Wait for other participants
            {
                Debug("Waiting for other participants");
                replicate = false;
                // Delay response to client
                resRequestIDs.erase(resRequestIDs.begin());
            }
            break;
        case strongstore::proto::Request::COMMIT:
            replicate = true;
            str2 = str1;
            break;
        case strongstore::proto::Request::ABORT:
            replicate = true;
            str2 = str1;
            break;
        default:
            Panic("Unrecognized operation.");
        }
    }

    /* Gets called when a command is issued using client.Invoke(...) to this
 * replica group. 
 * opnum is the operation number.
 * str1 is the request string passed by the client.
 * str2 is the reply which will be sent back to the client.
 */
    void
    Server::ReplicaUpcall(opnum_t opnum,
                          const string &str1,
                          string &str2,
                          std::unordered_set<replication::RequestID> &resRequestIDs)
    {
        Debug("Received Upcall: %lu %s", opnum, str1.c_str());
        Request request;
        Reply reply;
        std::unordered_set<replication::RequestID> requestIDs;
        int status = REPLY_OK;

        request.ParseFromString(str1);

        switch (request.op())
        {
        case strongstore::proto::Request::GET:
            return;
        case strongstore::proto::Request::PREPARE:
            // get a prepare timestamp and return to client
            store->Prepare(request.txnid(), Transaction(request.prepare().txn()));

            if (AmLeader)
            {
                shardClient.PrepareOK(request.prepare().coordinatorshard(), request.txnid(), groupIdx, request.prepare().timestamp());
            }

            // Reply to client
            break;
        case strongstore::proto::Request::COMMIT:
            Debug("Received COMMIT");
            if (request.has_prepare()) // Fast path commit
            {
                Debug("Fast path COMMIT");
                store->Prepare(request.txnid(), Transaction(request.prepare().txn()));
            }
            if (store->Commit(request.txnid(), request.commit().timestamp()))
            {
                timeMaxWrite = std::max(timeMaxWrite, request.commit().timestamp());
            }

            Debug("AmLeader: %d", AmLeader);
            if (AmLeader) {
                Debug("Replying to client and shards");
                // Reply to client and shards
                reply.set_timestamp(request.commit().timestamp());
                requestIDs = coordinator.GetRequestIDs(request.txnid());
                resRequestIDs.insert(requestIDs.begin(), requestIDs.end());
            } else {
                // Only leader needs to respond to client
                resRequestIDs.erase(resRequestIDs.begin());
            }

            Debug("COMMIT timestamp: %lu", request.commit().timestamp());
            coordinator.Commit(request.txnid());
            break;
        case strongstore::proto::Request::ABORT:
            store->Abort(request.txnid(), Transaction(request.abort().txn()));
            break;
        default:
            Panic("Unrecognized operation.");
        }
        reply.set_status(status);
        reply.SerializeToString(&str2);
    }

    void
    Server::UnloggedUpcall(const string &str1, string &str2)
    {
        Request request;
        Reply reply;
        int status;

        request.ParseFromString(str1);

        ASSERT(request.op() == strongstore::proto::Request::GET);

        if (request.get().has_timestamp())
        {
            pair<Timestamp, string> val;
            status = store->Get(request.txnid(), request.get().key(),
                                request.get().timestamp(), val);
            if (status == 0)
            {
                reply.set_value(val.second);
            }
        }
        else
        {
            pair<Timestamp, string> val;
            status = store->Get(request.txnid(), request.get().key(), val);
            if (status == 0)
            {
                reply.set_value(val.second);
                reply.set_timestamp(val.first.getTimestamp());
            }
        }

        reply.set_status(status);
        reply.SerializeToString(&str2);
    }

    void
    Server::Load(const string &key, const string &value, const Timestamp timestamp)
    {
        store->Load(key, value, timestamp);
    }

} // namespace strongstore
