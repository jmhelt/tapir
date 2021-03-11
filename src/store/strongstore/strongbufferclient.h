// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/bufferclient.h:
 *   Single shard buffering client implementation.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#ifndef _STRONG_BUFFER_CLIENT_H_
#define _STRONG_BUFFER_CLIENT_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/promise.h"
#include "store/common/transaction.h"
#include "store/strongstore/shardclient.h"

namespace strongstore {
class BufferClient : public ::BufferClient {
   public:
    BufferClient(ShardClient *shard_client);
    ~BufferClient();

    void Begin(uint64_t tid, const Timestamp &start_time);

    void RWCommitCoordinator(uint64_t transaction_id, int n_participants,
                             prepare_callback pcb,
                             prepare_timeout_callback ptcb, uint32_t timeout);

    // Prepare (Spanner requires a prepare timestamp)
    void Prepare(uint64_t id, int coordShard, int nParticipants,
                 prepare_callback pcb, prepare_timeout_callback ptcb,
                 uint32_t timeout);

   private:
    // Underlying single shard transaction client implementation.
    ShardClient *shard_client_;
};
};     // namespace strongstore
#endif /* _BUFFER_CLIENT_H_ */
