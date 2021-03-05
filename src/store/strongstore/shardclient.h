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
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {
// Timeouts for various operations
const int GET_TIMEOUT = 250;
const int GET_RETRIES = 3;
// Only used for QWStore
const int PUT_TIMEOUT = 250;
const int PREPARE_TIMEOUT = 1000;
const int PREPARE_RETRIES = 5;

const int COMMIT_TIMEOUT = 1000;
const int COMMIT_RETRIES = 5;

const int ABORT_TIMEOUT = 1000;
const int RETRY_TIMEOUT = 500000;

enum Mode {
    MODE_UNKNOWN,
    MODE_OCC,
    MODE_LOCK,
    MODE_SPAN_OCC,
    MODE_SPAN_LOCK,
    MODE_MVTSO
};

class ShardClient {
   public:
    /* Constructor needs path to shard config. */
    ShardClient(Mode mode, transport::Configuration *config,
                Transport *transport, uint64_t client_id, int shard,
                int closestReplica);
    ~ShardClient();

    // Overriding from TxnClient
    void Begin(uint64_t id);
    void Get(uint64_t id, const std::string &key, Promise *promise = nullptr);
    void Get(uint64_t id, const std::string &key, const Timestamp &timestamp,
             Promise *promise = nullptr);
    void Put(uint64_t id, const std::string &key, const std::string &value,
             Promise *promise = nullptr);
    void Prepare(uint64_t id, const Transaction &txn, int coordShard,
                 int nParticipants, Promise *promise = nullptr);
    void PrepareOK(uint64_t id, int participantShard, uint64_t prepareTS,
                   replication::Client::continuation_t continuation);
    void PrepareAbort(uint64_t id, int participantShard,
                      replication::Client::continuation_t continuation);
    void Commit(int coordShard, uint64_t id, uint64_t timestamp);
    void Abort(int coordShard, uint64_t id);

   private:
    Transport *transport;  // Transport layer.
    uint64_t client_id;    // Unique ID for this client.
    int shard;             // which shard this client accesses
    int replica;           // which replica to use for reads

    replication::vr::VRClient *client;  // Client proxy.
    Promise *blockingBegin;             // block until finished

    /* Timeout for Get requests, which only go to one replica. */
    void GetTimeout(Promise *promise);

    /* Callbacks for hearing back from a shard for an operation. */
    void GetCallback(Promise *promise, const std::string &,
                     const std::string &);
    void PrepareCallback(Promise *promise, const std::string &,
                         const std::string &);
    void CommitCallback(const std::string &, const std::string &);
    void AbortCallback(const std::string &, const std::string &);

    /* Helper Functions for starting and finishing requests */
    void StartRequest();
    void WaitForResponse();
    void FinishRequest(const std::string &reply_str);
    void FinishRequest();
    int SendGet(const std::string &request_str);
};

}  // namespace strongstore

#endif /* _STRONG_SHARDCLIENT_H_ */
