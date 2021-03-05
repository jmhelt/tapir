// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/client.h:
 *   Transactional client interface.
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
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

#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <set>
#include <thread>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"
#include "store/strongstore/strongbufferclient.h"

namespace strongstore {

class Client : public ::Client {
   public:
    Client(Mode mode, transport::Configuration *config, uint64_t id,
           uint64_t nshards, int closestReplica, Transport *transport,
           Partitioner *part);
    ~Client();

    // Overriding functions from ::Client
    void Begin();
    int Get(const string &key, string &value);
    int Put(const string &key, const string &value);
    bool Commit();
    void Abort();
    std::vector<int> Stats();

   private:
    // local Prepare function
    int Prepare(uint64_t &ts);

    // choose coordinator from participants
    int ChooseCoordinator(const std::set<int> &participants);

    transport::Configuration *config;
    // Unique ID for this client.
    uint64_t client_id;

    // Ongoing transaction ID.
    uint64_t t_id;

    // Number of shards in SpanStore.
    uint64_t nshards;

    // List of participants in the ongoing transaction.
    std::set<int> participants;

    // Transport used by paxos client proxies.
    Transport *transport;

    // Buffering client for each shard.
    std::vector<BufferClient *> bclient;
    std::vector<ShardClient *> sclient;

    // Mode in which spanstore runs.
    Mode mode;

    // Partitioner
    Partitioner *part;

    uint64_t lastReqId;
    std::unordered_map<std::string, uint32_t> statInts;

    Latency_t opLat;
};

}  // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
