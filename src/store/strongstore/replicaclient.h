#ifndef _STRONG_REPLICACLIENT_H_
#define _STRONG_REPLICACLIENT_H_

#include <unordered_map>

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

class ReplicaClient {
   public:
    /* Constructor needs path to shard config. */
    ReplicaClient(const transport::Configuration &config, Transport *transport,
                  uint64_t client_id, int shard);
    virtual ~ReplicaClient();

    void FastPathCommit(uint64_t transaction_id, const Transaction transaction,
                        uint64_t commit_timestamp, commit_callback ccb,
                        commit_timeout_callback ctcb, uint32_t timeout);

    void Commit(uint64_t transaction_id, uint64_t commit_timestamp,
                commit_callback ccb, commit_timeout_callback ctcb,
                uint32_t timeout);

   private:
    struct PendingRequest {
        PendingRequest(uint64_t reqId) : reqId(reqId) {}
        uint64_t reqId;
    };
    struct PendingCommit : public PendingRequest {
        PendingCommit(uint64_t reqId) : PendingRequest(reqId) {}
        commit_callback ccb;
        commit_timeout_callback ctcb;
    };

    bool CommitCallback(uint64_t reqId, const std::string &,
                        const std::string &);

    const transport::Configuration &config_;
    Transport *transport_;  // Transport layer.
    uint64_t client_id_;    // Unique ID for this client.
    int shard_idx_;         // which shard this client accesses

    replication::vr::VRClient *client;  // Client proxy.

    std::unordered_map<uint64_t, PendingCommit *> pendingCommits;

    uint64_t lastReqId;
};

}  // namespace strongstore

#endif /* _STRONG_REPLICACLIENT_H_ */
