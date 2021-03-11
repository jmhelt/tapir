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

#include <unordered_map>
#include <unordered_set>

#include "lib/latency.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "replication/vr/replica.h"
#include "store/common/backend/messageserver.h"
#include "store/common/truetime.h"
#include "store/server.h"
#include "store/strongstore/coordinator.h"
#include "store/strongstore/intershardclient.h"
#include "store/strongstore/lockstore.h"
#include "store/strongstore/occstore.h"
#include "store/strongstore/replicaclient.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class Server : public replication::AppReplica, public MessageServer {
   public:
    Server(const transport::Configuration &shard_config,
           const transport::Configuration &replica_config, uint64_t server_id,
           int groupIdx, int idx, Transport *transport,
           InterShardClient &shardClient, const TrueTime &tt, bool debug_stats);
    virtual ~Server();

    virtual void LeaderUpcall(opnum_t opnum, const string &op, bool &replicate,
                              string &response,
                              std::unordered_set<replication::RequestID>
                                  &response_request_ids) override;
    virtual void ReplicaUpcall(
        opnum_t opnum, const string &op, string &response,
        std::unordered_set<replication::RequestID> &response_request_ids,
        uint64_t &response_delay_ms) override;

    virtual void UnloggedUpcall(const string &op, string &response) override;
    virtual void LeaderStatusUpcall(bool l) override {
        Debug("Updating AmLeader: %d", l);
        AmLeader = l;
    }
    void Load(const string &key, const string &value,
              const Timestamp timestamp) override;

   private:
    class PendingReply {
       public:
        PendingReply(uint64_t client_id, uint64_t client_req_id)
            : client_id(client_id), client_req_id(client_req_id) {}
        uint64_t client_id;
        uint64_t client_req_id;
    };
    class PendingRWCommitCoordinatorReply : public PendingReply {
       public:
        PendingRWCommitCoordinatorReply(uint64_t client_id,
                                        uint64_t client_req_id,
                                        TransportAddress *remote)
            : PendingReply(client_id, client_req_id), remote{remote} {}
        TransportAddress *remote;
        uint64_t commit_timestamp;
    };
    class PendingRWCommitParticipantReply : public PendingReply {
       public:
        PendingRWCommitParticipantReply(uint64_t client_id,
                                        uint64_t client_req_id,
                                        TransportAddress *remote)
            : PendingReply(client_id, client_req_id), remote{remote} {}
        TransportAddress *remote;
        uint64_t prepare_timestamp;
        int coordinator_shard;
    };
    class PendingPrepareOKReply : public PendingReply {
       public:
        PendingPrepareOKReply(uint64_t client_id, uint64_t client_req_id,
                              TransportAddress *remote)
            : PendingReply(client_id, client_req_id), remotes{remote} {}
        std::vector<TransportAddress *> remotes;
        uint64_t commit_timestamp;
    };

    void HandleGet(const TransportAddress &remote,
                   google::protobuf::Message *msg);

    void HandleRWCommitCoordinator(const TransportAddress &remote,
                                   google::protobuf::Message *msg);

    void HandleRWCommitParticipant(const TransportAddress &remote,
                                   google::protobuf::Message *m);

    void HandlePrepareOK(const TransportAddress &remote,
                         google::protobuf::Message *m);

    void PrepareCallback(uint64_t transaction_id, int status,
                         Timestamp timestamp);
    void PrepareOKCallback(uint64_t transaction_id, int status,
                           Timestamp timestamp);
    void CommitCallback(uint64_t transaction_id, transaction_status_t status);

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

    proto::Get get_;
    proto::RWCommitCoordinator rw_commit_c_;
    proto::RWCommitParticipant rw_commit_p_;
    proto::PrepareOK prepare_ok_;

    proto::GetReply get_reply_;
    proto::RWCommitCoordinatorReply rw_commit_c_reply_;
    proto::RWCommitParticipantReply rw_commit_p_reply_;
    proto::PrepareOKReply prepare_ok_reply_;

    Latency_t prepare_lat_;
    Latency_t commit_lat_;

    InterShardClient &shardClient;
    uint64_t timeMaxWrite;
    int shard_idx_;
    int replica_idx_;
    TxnStore *store;
    bool AmLeader;
    bool debug_stats_;
};

}  // namespace strongstore

#endif /* _STRONG_SERVER_H_ */
