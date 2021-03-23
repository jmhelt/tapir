// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/server.h:
 *   A single transactional server replica.
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

#ifndef _STRONG_SERVER_H_
#define _STRONG_SERVER_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "lib/latency.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "replication/vr/replica.h"
#include "store/common/backend/pingserver.h"
#include "store/common/truetime.h"
#include "store/server.h"
#include "store/strongstore/coordinator.h"
#include "store/strongstore/lockstore.h"
#include "store/strongstore/occstore.h"
#include "store/strongstore/replicaclient.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class RequestID {
   public:
    RequestID(uint64_t client_id, uint64_t client_req_id,
              TransportAddress *addr)
        : client_id_{client_id}, client_req_id_{client_req_id}, addr_{addr} {}
    ~RequestID() {}

    const uint64_t client_id() const { return client_id_; }
    const uint64_t client_req_id() const { return client_req_id_; }
    const TransportAddress *addr() const { return addr_; }

   private:
    uint64_t client_id_;
    uint64_t client_req_id_;
    TransportAddress *addr_;
};

inline bool operator==(const strongstore::RequestID &lhs,
                       const strongstore::RequestID &rhs) {
    return lhs.client_id() == rhs.client_id() &&
           lhs.client_req_id() == rhs.client_req_id();
}
}  // namespace strongstore

namespace std {
template <>
struct hash<strongstore::RequestID> {
    std::size_t operator()(strongstore::RequestID const &rid) const noexcept {
        std::size_t h1 = std::hash<std::uint64_t>{}(rid.client_id());
        std::size_t h2 = std::hash<std::uint64_t>{}(rid.client_req_id());
        return h1 ^ (h2 << 1);  // or use boost::hash_combine
    }
};
}  // namespace std

namespace strongstore {

class Server : public TransportReceiver,
               public ::Server,
               public replication::AppReplica,
               public PingServer {
   public:
    Server(Consistency consistency,
           const transport::Configuration &shard_config,
           const transport::Configuration &replica_config, uint64_t server_id,
           int groupIdx, int idx, Transport *transport, const TrueTime &tt,
           bool debug_stats);
    virtual ~Server();

    // Override TransportReceiver
    virtual void ReceiveMessage(const TransportAddress &remote,
                                const std::string &type,
                                const std::string &data,
                                void *meta_data) override;

    // Override AppReplica
    virtual void LeaderUpcall(opnum_t opnum, const string &op, bool &replicate,
                              string &response) override;
    virtual void ReplicaUpcall(opnum_t opnum, const string &op,
                               string &response) override;

    virtual void UnloggedUpcall(const string &op, string &response) override;

    // Override Server
    void Load(const string &key, const string &value,
              const Timestamp timestamp) override;
    virtual inline Stats &GetStats() override { return stats; };

   private:
    class PendingRWCommitCoordinatorReply {
       public:
        PendingRWCommitCoordinatorReply(uint64_t client_id,
                                        uint64_t client_req_id,
                                        TransportAddress *remote)
            : rid{client_id, client_req_id, remote} {}
        RequestID rid;
        Timestamp commit_timestamp;
        Timestamp nonblock_timestamp;
    };
    class PendingRWCommitParticipantReply {
       public:
        PendingRWCommitParticipantReply(uint64_t client_id,
                                        uint64_t client_req_id,
                                        TransportAddress *remote)
            : rid{client_id, client_req_id, remote} {}
        RequestID rid;
        Timestamp prepare_timestamp;
        int coordinator_shard;
    };
    class PendingPrepareOKReply {
       public:
        PendingPrepareOKReply(uint64_t client_id, uint64_t client_req_id,
                              TransportAddress *remote)
            : rids{{client_id, client_req_id, remote}} {}
        std::unordered_set<RequestID> rids;
    };
    class PendingROCommitReply {
       public:
        PendingROCommitReply(uint64_t client_id, uint64_t client_req_id,
                             TransportAddress *remote)
            : rid{client_id, client_req_id, remote} {}
        RequestID rid;
        uint64_t transaction_id;
        Timestamp commit_timestamp;
        uint64_t n_waiting_prepared;
        std::unordered_set<std::string> keys;
    };

    void HandleGet(const TransportAddress &remote, proto::Get &msg);

    void HandleROCommit(const TransportAddress &remote, proto::ROCommit &msg);

    void HandleRWCommitCoordinator(const TransportAddress &remote,
                                   proto::RWCommitCoordinator &msg);

    void SendROCommitReply(PendingROCommitReply *reply);

    void SendRWCommmitCoordinatorReplyOK(PendingRWCommitCoordinatorReply *reply,
                                         uint64_t response_delay_ms);
    void SendRWCommmitCoordinatorReplyFail(const TransportAddress &remote,
                                           uint64_t client_id,
                                           uint64_t client_req_id);

    void SendRWCommmitParticipantReplyOK(
        PendingRWCommitParticipantReply *reply);
    void SendRWCommmitParticipantReplyFail(const TransportAddress &remote,
                                           uint64_t client_id,
                                           uint64_t client_req_id);

    void SendPrepareOKRepliesOK(PendingPrepareOKReply *reply,
                                Timestamp &commit_timestamp,
                                uint64_t response_delay_ms);
    void SendPrepareOKRepliesFail(PendingPrepareOKReply *reply);

    void HandleRWCommitParticipant(const TransportAddress &remote,
                                   proto::RWCommitParticipant &msg);

    void HandlePrepareOK(const TransportAddress &remote, proto::PrepareOK &msg);
    void HandlePrepareAbort(const TransportAddress &remote,
                            proto::PrepareAbort &msg);

    void PrepareCallback(uint64_t transaction_id, int status,
                         Timestamp timestamp);
    void PrepareOKCallback(uint64_t transaction_id, int status,
                           Timestamp timestamp);
    void PrepareAbortCallback(uint64_t transaction_id, int status,
                              Timestamp timestamp);

    void CommitCoordinatorCallback(
        uint64_t transaction_id, transaction_status_t status,
        const std::unordered_set<uint64_t> &notify_ros);
    void CommitParticipantCallback(
        uint64_t transaction_id, transaction_status_t status,
        const std::unordered_set<uint64_t> &notify_ros);
    void AbortParticipantCallback(
        uint64_t transaction_id,
        const std::unordered_set<uint64_t> &notify_ros);

    void NotifyPendingROs(const std::unordered_set<uint64_t> &ros);
    bool NotifyPendingRO(PendingROCommitReply *reply);

    const transport::Configuration &shard_config_;
    const transport::Configuration &replica_config_;

    std::vector<ShardClient *> shard_clients_;
    ReplicaClient *replica_client_;

    Transport *transport_;
    const TrueTime &tt_;
    Coordinator coordinator;

    uint64_t server_id_;

    std::unordered_map<uint64_t, PendingRWCommitCoordinatorReply *>
        pending_rw_commit_c_replies_;
    std::unordered_map<uint64_t, PendingRWCommitParticipantReply *>
        pending_rw_commit_p_replies_;
    std::unordered_map<uint64_t, PendingPrepareOKReply *>
        pending_prepare_ok_replies_;
    std::unordered_map<uint64_t, PendingROCommitReply *>
        pending_ro_commit_repies_;

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
    PingMessage ping_;

    Stats stats;

    Latency_t prepare_lat_;
    Latency_t commit_lat_;

    Timestamp max_write_timestamp_;
    int shard_idx_;
    int replica_idx_;
    LockStore *store_;
    Consistency consistency_;
    bool debug_stats_;
};

}  // namespace strongstore

#endif /* _STRONG_SERVER_H_ */
