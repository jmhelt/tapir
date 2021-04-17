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

#include <set>
#include <vector>

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/pinginitiator.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/preparedtransaction.h"
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

typedef std::function<void(int, const std::vector<Value> &, const std::vector<PreparedTransaction> &)> ro_commit_callback;

typedef std::function<void(int, uint64_t, const Timestamp &, bool)> ro_commit_slow_callback;

typedef std::function<void()> ro_commit_timeout_callback;

typedef std::function<void(uint64_t)> wound_callback;

class ShardClient : public TxnClient,
                    public TransportReceiver,
                    public PingInitiator,
                    public PingTransport {
   public:
    /* Constructor needs path to shard config. */
    ShardClient(
        const transport::Configuration &config, Transport *transport,
        uint64_t client_id, int shard, wound_callback wcb = [](uint64_t transaction_id) {});
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

    void ROCommit(uint64_t transaction_id, const std::vector<std::string> &keys,
                  const Timestamp &commit_timestamp,
                  const Timestamp &min_read_timestamp,
                  ro_commit_callback ccb, ro_commit_slow_callback cscb,
                  ro_commit_timeout_callback ctcb, uint32_t timeout);

    void RWCommitCoordinator(uint64_t transaction_id,
                             const Transaction &transaction,
                             const std::set<int> participants,
                             Timestamp &nonblock_timestamp,
                             prepare_callback pcb,
                             prepare_timeout_callback ptcb, uint32_t timeout);
    void RWCommitParticipant(uint64_t transaction_id,
                             const Transaction &transaction,
                             int coordinator_shard,
                             Timestamp &nonblock_timestamp,
                             prepare_callback pcb,
                             prepare_timeout_callback ptcb, uint32_t timeout);

    void PrepareOK(uint64_t transaction_id, int participant_shard,
                   const Timestamp &prepare_timestamp, prepare_callback pcb,
                   prepare_timeout_callback ptcb, uint32_t timeout);

    void PrepareAbort(uint64_t transaction_id, int participant_shard,
                      prepare_callback pcb, prepare_timeout_callback ptcb,
                      uint32_t timeout);

    virtual void Abort(uint64_t id, const Transaction &txn, abort_callback acb,
                       abort_timeout_callback atcb, uint32_t timeout) override;
    void Abort(uint64_t transaction_id, abort_callback acb,
               abort_timeout_callback atcb, uint32_t timeout);

    void Wound(uint64_t transaction_id);

    // Override PingInitiator
    virtual bool SendPing(size_t replica, const PingMessage &ping);

    uint64_t GetLatencyToLeader();

    // Unimplemented
    virtual void Put(uint64_t id, const std::string &key,
                     const std::string &value, put_callback pcb,
                     put_timeout_callback ptcb, uint32_t timeout) override;
    virtual void Commit(uint64_t id, const Transaction &txn,
                        const Timestamp &timestamp, commit_callback ccb,
                        commit_timeout_callback ctcb,
                        uint32_t timeout) override;
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
        ro_commit_callback ccb;
        ro_commit_slow_callback cscb;
        ro_commit_timeout_callback ctcb;
        uint64_t n_slow_replies;
    };

    void HandleGetReply(const proto::GetReply &reply);
    void HandleRWCommitCoordinatorReply(
        const proto::RWCommitCoordinatorReply &reply);
    void HandleRWCommitParticipantReply(
        const proto::RWCommitParticipantReply &reply);
    void HandlePrepareOKReply(const proto::PrepareOKReply &reply);
    void HandlePrepareAbortReply(const proto::PrepareAbortReply &reply);
    void HandleROCommitReply(const proto::ROCommitReply &reply);
    void HandleROCommitSlowReply(const proto::ROCommitSlowReply &reply);
    void HandleAbortReply(const proto::AbortReply &reply);
    void HandleWound(const proto::Wound &wound);

    const transport::Configuration &config_;
    Transport *transport_;  // Transport layer.
    uint64_t client_id_;    // Unique ID for this client.
    int shard_idx_;         // which shard this client accesses
    int replica_;           // which replica to use for reads
    wound_callback wcb_;

    std::unordered_map<uint64_t, PendingGet *> pendingGets;
    std::unordered_map<uint64_t, PendingPrepare *> pendingPrepares;
    std::unordered_map<uint64_t, PendingPrepareOK *> pendingPrepareOKs;
    std::unordered_map<uint64_t, PendingPrepareAbort *> pendingPrepareAborts;
    std::unordered_map<uint64_t, PendingCommit *> pendingCommits;
    std::unordered_map<uint64_t, PendingAbort *> pendingAborts;
    std::unordered_map<uint64_t, PendingROCommit *> pendingROCommits;

    proto::Get get_;
    proto::RWCommitCoordinator rw_commit_c_;
    proto::RWCommitParticipant rw_commit_p_;
    proto::PrepareOK prepare_ok_;
    proto::PrepareAbort prepare_abort_;
    proto::ROCommit ro_commit_;
    proto::Abort abort_;
    proto::Wound wound_;

    proto::GetReply get_reply_;
    proto::RWCommitCoordinatorReply rw_commit_c_reply_;
    proto::RWCommitParticipantReply rw_commit_p_reply_;
    proto::PrepareOKReply prepare_ok_reply_;
    proto::PrepareAbortReply prepare_abort_reply_;
    proto::ROCommitReply ro_commit_reply_;
    proto::ROCommitSlowReply ro_commit_slow_reply_;
    proto::AbortReply abort_reply_;
    PingMessage ping_;
};

}  // namespace strongstore

#endif /* _STRONG_SHARDCLIENT_H_ */
