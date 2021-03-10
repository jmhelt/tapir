// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/bufferclient.cc:
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

#include "store/strongstore/strongbufferclient.h"

using namespace std;

namespace strongstore {

BufferClient::BufferClient(ShardClient *shard_client)
    : ::BufferClient(shard_client, true), shard_client_{shard_client} {}

BufferClient::~BufferClient() {}

/* Begins a transaction. */
void BufferClient::Begin(uint64_t tid, const Timestamp &start_time) {
    // Initialize data structures.
    txn = Transaction();
    txn.set_start_time(start_time);
    readSet.clear();
    this->tid = tid;
    txnclient->Begin(tid);
}

/* Prepare the transaction. */
void BufferClient::Prepare(uint64_t id, int coordShard, int nParticipants,
                           prepare_callback pcb, prepare_timeout_callback ptcb,
                           uint32_t timeout) {
    shard_client_->Prepare(tid, txn, coordShard, nParticipants, pcb, ptcb,
                           timeout);
}
};  // namespace strongstore