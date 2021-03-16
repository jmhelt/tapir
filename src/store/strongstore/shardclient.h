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

class ShardClient : public TxnClient, public TransportReceiver {
   public:
    /* Constructor needs path to shard config. */
    ShardClient(const transport::Configuration &config, Transport *transport,
                uint64_t client_id, int shard);
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

    void ROCommit(uint64_t transaction_id, const Transaction &transaction,
                  commit_callback ccb, commit_timeout_callback ctcb,
                  uint32_t timeout);

    void RWCommitCoordinator(uint64_t transaction_id,
                             const Transaction &transaction, int n_participants,
                             prepare_callback pcb,
                             prepare_timeout_callback ptcb, uint32_t timeout);
    void RWCommitParticipant(uint64_t transaction_id,
                             const Transaction &transaction,
                             int coordinator_shard, prepare_callback pcb,
                             prepare_timeout_callback ptcb, uint32_t timeout);

    void PrepareOK(uint64_t transaction_id, int participant_shard,
                   uint64_t prepare_timestamp, prepare_callback pcb,
                   prepare_timeout_callback ptcb, uint32_t timeout);

    void PrepareAbort(uint64_t transaction_id, int participant_shard,
                      prepare_callback pcb, prepare_timeout_callback ptcb,
                      uint32_t timeout);

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
    struct PendingPrepareOK : public PendingRequest {
        PendingPrepareOK(uint64_t reqId) : PendingRequest(reqId) {}
        prepare_callback pcb;
        prepare_timeout_callback ptcb;
    };
    struct PendingPrepareAbort : public PendingRequest {
        PendingPrepareAbort(uint64_t reqId) : PendingRequest(reqId) {}
        prepare_callback pcb;
        prepare_timeout_callback ptcb;
    };
    struct PendingROCommit : public PendingRequest {
        PendingROCommit(uint64_t reqId) : PendingRequest(reqId) {}
        commit_callback ccb;
        commit_timeout_callback ctcb;
    };

    void HandleGetReply(const proto::GetReply &reply);
    void HandleRWCommitCoordinatorReply(
        const proto::RWCommitCoordinatorReply &reply);
    void HandleRWCommitParticipantReply(
        const proto::RWCommitParticipantReply &reply);
    void HandlePrepareOKReply(const proto::PrepareOKReply &reply);
    void HandlePrepareAbortReply(const proto::PrepareAbortReply &reply);
    void HandleROCommitReply(const proto::ROCommitReply &reply);

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

    const transport::Configuration &config_;
    Transport *transport_;  // Transport layer.
    uint64_t client_id_;    // Unique ID for this client.
    int shard_idx_;         // which shard this client accesses
    int replica_;           // which replica to use for reads

    std::unordered_map<uint64_t, PendingGet *> pendingGets;
    std::unordered_map<uint64_t, PendingPrepare *> pendingPrepares;
    std::unordered_map<uint64_t, PendingPrepareOK *> pendingPrepareOKs;
    std::unordered_map<uint64_t, PendingPrepareAbort *> pendingPrepareAborts;
    std::unordered_map<uint64_t, PendingCommit *> pendingCommits;
    std::unordered_map<uint64_t, PendingAbort *> pendingAborts;
    std::unordered_map<uint64_t, PendingROCommit *> pendingROCommits;
    Latency_t opLat;

    proto::Get get_;
    proto::RWCommitCoordinator rw_commit_c_;
    proto::RWCommitParticipant rw_commit_p_;
    proto::PrepareOK prepare_ok_;
    proto::PrepareAbort prepare_abort_;
    proto::ROCommit ro_commit_;

    proto::GetReply get_reply_;
    proto::RWCommitCoordinatorReply rw_commit_c_reply_;
    proto::RWCommitParticipantReply rw_commit_p_reply_;
    proto::PrepareOKReply prepare_ok_reply_;
    proto::PrepareAbortReply prepare_abort_reply_;
    proto::ROCommitReply ro_commit_reply_;
};

}  // namespace strongstore

#endif /* _STRONG_SHARDCLIENT_H_ */
