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

void BufferClient::RWCommitCoordinator(uint64_t transaction_id,
                                       int n_participants, prepare_callback pcb,
                                       prepare_timeout_callback ptcb,
                                       uint32_t timeout) {
    shard_client_->RWCommitCoordinator(transaction_id, txn, n_participants, pcb,
                                       ptcb, timeout);
}

void BufferClient::RWCommitParticipant(uint64_t transaction_id,
                                       int coordinator_shard,
                                       prepare_callback pcb,
                                       prepare_timeout_callback ptcb,
                                       uint32_t timeout) {
    shard_client_->RWCommitParticipant(transaction_id, txn, coordinator_shard,
                                       pcb, ptcb, timeout);
}

void BufferClient::AddReadSet(const std::string &key,
                              const Timestamp &timestamp) {
    this->txn.addReadSet(key, timestamp);
    this->readSet.insert(std::make_pair(key, std::make_tuple("", timestamp)));
}

void BufferClient::ROCommit(commit_callback ccb, commit_timeout_callback ctcb,
                            uint32_t timeout) {
    shard_client_->ROCommit(tid, txn, ccb, ctcb, timeout);
}
};  // namespace strongstore