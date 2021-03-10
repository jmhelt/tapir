// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/strongclient.h:
 *   Single shard strong consistency transactional client interface.
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

#ifndef _STRONG_SHARDCLIENT_H_
#define _STRONG_SHARDCLIENT_H_

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

enum Mode {
    MODE_UNKNOWN,
    MODE_OCC,
    MODE_LOCK,
    MODE_SPAN_OCC,
    MODE_SPAN_LOCK,
    MODE_MVTSO
};

// typedef std::function<void(int, const std::string &, const std::string &,
//                            const ::mortystore::Version &, uint64_t,
//                            uint64_t)>
//     get_callback;
// typedef std::function<void(int, const std::string &)> get_timeout_callback;

class ShardClient : public TxnClient, public TransportReceiver {
   public:
    /* Constructor needs path to shard config. */
    ShardClient(transport::Configuration *config, Transport *transport,
                uint64_t client_id, int shard, int closestReplica);
    virtual ~ShardClient();

    virtual void ReceiveMessage(const TransportAddress &remote,
                                const std::string &type,
                                const std::string &data,
                                void *meta_data) override;

    void Begin(uint64_t id) override;
    virtual void Get(uint64_t id, const std::string &key, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) override;
    virtual void Get(uint64_t id, const std::string &key,
                     const Timestamp &timestamp, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) override;

    void Prepare(uint64_t id, const Transaction &txn, int coordShard,
                 int nParticipants, prepare_callback pcb,
                 prepare_timeout_callback ptcb, uint32_t timeout);
    void PrepareOK(uint64_t id, int participantShard, uint64_t prepareTS,
                   replication::Client::continuation_t continuation);
    void PrepareAbort(uint64_t id, int participantShard,
                      replication::Client::continuation_t continuation);
    void Commit(int coordShard, uint64_t id, uint64_t timestamp,
                commit_callback ccb, commit_timeout_callback ctcb,
                uint32_t timeout);
    void Abort(int coordShard, uint64_t id, abort_callback acb,
               abort_timeout_callback atcb, uint32_t timeout);

    // Unimplemented
    virtual void Put(uint64_t id, const std::string &key,
                     const std::string &value, put_callback pcb,
                     put_timeout_callback ptcb, uint32_t timeout) override;
    virtual void Commit(uint64_t id, const Transaction &txn,
                        const Timestamp &timestamp, commit_callback ccb,
                        commit_timeout_callback ctcb,
                        uint32_t timeout) override;
    virtual void Abort(uint64_t id, const Transaction &txn, abort_callback acb,
                       abort_timeout_callback atcb, uint32_t timeout) override;
    virtual void Prepare(uint64_t id, const Transaction &txn,
                         const Timestamp &timestamp, prepare_callback pcb,
                         prepare_timeout_callback ptcb,
                         uint32_t timeout) override;

   private:
    void HandleGetReply(const proto::GetReply &reply);

    /* Timeout for Get requests, which only go to one replica. */
    void GetTimeout(uint64_t reqId);

    /* Callbacks for hearing back from a shard for an operation. */
    bool GetCallback(uint64_t reqId, const std::string &, const std::string &);
    bool PrepareCallback(uint64_t reqId, const std::string &,
                         const std::string &);
    bool CommitCallback(uint64_t reqId, const std::string &,
                        const std::string &);
    bool AbortCallback(uint64_t reqId, const std::string &,
                       const std::string &);

    transport::Configuration *config_;
    Transport *transport_;  // Transport layer.
    uint64_t client_id_;    // Unique ID for this client.
    int shard_idx_;         // which shard this client accesses
    int replica;            // which replica to use for reads

    replication::vr::VRClient *client;  // Client proxy.

    std::unordered_map<uint64_t, PendingGet *> pendingGets;
    std::unordered_map<uint64_t, PendingPrepare *> pendingPrepares;
    std::unordered_map<uint64_t, PendingCommit *> pendingCommits;
    std::unordered_map<uint64_t, PendingAbort *> pendingAborts;
    Latency_t opLat;

    proto::Get get_;

    proto::GetReply get_reply_;
};

}  // namespace strongstore

#endif /* _STRONG_SHARDCLIENT_H_ */
