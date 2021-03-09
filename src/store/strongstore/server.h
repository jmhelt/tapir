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

#include "lib/latency.h"
#include "lib/transport.h"
#include "replication/vr/replica.h"
#include "store/common/truetime.h"
#include "store/server.h"
#include "store/strongstore/coordinator.h"
#include "store/strongstore/intershardclient.h"
#include "store/strongstore/lockstore.h"
#include "store/strongstore/occstore.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class Server : public replication::AppReplica, public ::Server {
   public:
    Server(InterShardClient &shardClient, int groupIdx, int myIdx, Mode mode,
           uint64_t error, bool debug_stats);
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
    virtual inline Stats &GetStats() override { return store->GetStats(); }

   private:
    TrueTime timeServer;
    Coordinator coordinator;

    Latency_t prepare_lat_;
    Latency_t commit_lat_;

    InterShardClient &shardClient;
    uint64_t timeMaxWrite;
    int groupIdx;
    int myIdx;
    Mode mode;
    TxnStore *store;
    bool AmLeader;
    bool debug_stats_;
};

}  // namespace strongstore

#endif /* _STRONG_SERVER_H_ */
