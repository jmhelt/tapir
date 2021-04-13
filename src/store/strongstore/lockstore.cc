// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/lockstore.h:
 *   Key-value store with support for strong consistency using S2PL
 *
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
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

#include "store/strongstore/lockstore.h"

#include "store/common/common.h"

using namespace std;

namespace strongstore {

LockStore::LockStore(Consistency consistency)
    : store_{},
      locks_{},
      prepared_{},
      waiting_{},
      aborted_{},
      stats_{},
      consistency_{consistency} {}
LockStore::~LockStore() {}

int LockStore::ContinuePrepare(uint64_t transaction_id,
                               const Timestamp &prepare_timestamp,
                               std::unordered_set<uint64_t> &notify_rws) {
    Debug("[%lu] Continue PREPARE", transaction_id);

    auto search = waiting_.find(transaction_id);
    if (search == waiting_.end()) {
        return REPLY_PREPARED;
    }

    PreparedTransaction &pt = search->second;
    const Transaction &transaction = pt.transaction();

    int status = getLocks(transaction_id, transaction);

    if (status == REPLY_OK) {
        Debug("[%lu] PREPARED TO COMMIT", transaction_id);
        pt.set_prepare_timestamp(prepare_timestamp);
        prepared_.emplace(transaction_id, std::move(pt));
        waiting_.erase(search);
    } else if (status == REPLY_FAIL) {
        dropLocks(transaction_id, transaction, notify_rws);
        waiting_.erase(search);
    }

    return status;
}

int LockStore::Prepare(uint64_t transaction_id, const Transaction &transaction,
                       const Timestamp &prepare_timestamp,
                       const Timestamp &nonblock_timestamp) {
    int status = Prepare(transaction_id, transaction, prepare_timestamp);
    if (status == REPLY_OK) {
        prepared_[transaction_id].set_nonblock_timestamp(nonblock_timestamp);
    } else if (status == REPLY_WAIT) {
        waiting_[transaction_id].set_nonblock_timestamp(nonblock_timestamp);
    }

    return status;
}

int LockStore::Prepare(uint64_t transaction_id, const Transaction &transaction,
                       const Timestamp &prepare_timestamp) {
    Debug("[%lu] START PREPARE", transaction_id);

    if (prepared_.size() > 100) {
        Warning("Lots of prepared transactions! %lu", prepared_.size());
    }

    if (prepared_.find(transaction_id) != prepared_.end()) {
        Debug("[%lu] Already prepared", transaction_id);
        return REPLY_OK;
    }

    if (aborted_.find(transaction_id) != aborted_.end()) {
        Debug("[%lu] Already aborted", transaction_id);
        return REPLY_FAIL;
    }

    int status = getLocks(transaction_id, transaction);

    PreparedTransaction pt{transaction_id, transaction};
    if (status == REPLY_OK) {
        Debug("[%lu] PREPARED TO COMMIT", transaction_id);
        pt.set_prepare_timestamp(prepare_timestamp);
        prepared_[transaction_id] = pt;
    } else if (status == REPLY_WAIT) {
        waiting_[transaction_id] = pt;
    }

    return status;
}

bool LockStore::Commit(uint64_t transaction_id, const Timestamp &timestamp,
                       std::unordered_set<uint64_t> &notify_rws,
                       std::unordered_set<uint64_t> &notify_ros) {
    Debug("[%lu] COMMIT", transaction_id);
    auto search = prepared_.find(transaction_id);
    if (search != prepared_.end()) {
        const PreparedTransaction &prepared = prepared_[transaction_id];

        const Transaction &transaction = prepared.transaction();

        for (auto &write : transaction.getWriteSet()) {
            store_.put(write.first, write.second, timestamp);
        }

        notify_ros = std::move(prepared.waiting_ros());

        // Drop locks.
        dropLocks(transaction_id, transaction, notify_rws);

        prepared_.erase(transaction_id);

        return !transaction.getWriteSet().empty();
    }

    return false;
}

bool LockStore::Abort(uint64_t transaction_id,
                      std::unordered_set<uint64_t> &notify_rws,
                      std::unordered_set<uint64_t> &notify_ros) {
    Debug("[%lu] ABORT", transaction_id);

    if (aborted_.find(transaction_id) != aborted_.end()) {
        Debug("[%lu] already aborted", transaction_id);
        return false;
    }

    bool prepared = false;
    auto search = waiting_.find(transaction_id);
    if (search != waiting_.end()) {
        const PreparedTransaction &pt = search->second;

        ASSERT(pt.waiting_ros().size() == 0);

        // Drop locks.
        dropLocks(transaction_id, pt.transaction(), notify_rws);

        waiting_.erase(search);
    }

    auto search2 = prepared_.find(transaction_id);
    if (search2 != prepared_.end()) {
        const PreparedTransaction &pt = search2->second;

        Debug("[%lu] waiting ros:", transaction_id);
        for (uint64_t waiting_ro : pt.waiting_ros()) {
            Debug("[%lu] waiting_ro: %lu", transaction_id, waiting_ro);
        }

        notify_ros = std::move(pt.waiting_ros());

        Debug("[%lu] notify_ros:", transaction_id);
        for (uint64_t waiting_ro : notify_ros) {
            Debug("[%lu] waiting_ro: %lu", transaction_id, waiting_ro);
        }

        // Drop locks.
        dropLocks(transaction_id, pt.transaction(), notify_rws);

        prepared_.erase(search2);

        prepared = true;
    }

    aborted_.insert(transaction_id);

    return prepared;
}

void LockStore::ReleaseLocks(uint64_t transaction_id,
                             const Transaction &transaction,
                             std::unordered_set<uint64_t> &notify_rws) {
    Debug("[%lu] ReleaseLocks", transaction_id);
    ASSERT(prepared_.find(transaction_id) == prepared_.end());

    // Drop locks.
    dropLocks(transaction_id, transaction, notify_rws);
}

const Transaction &LockStore::GetPreparedTransaction(
    uint64_t transaction_id) const {
    Debug("[%lu] GetPreparedTransaction", transaction_id);

    auto search = prepared_.find(transaction_id);
    ASSERT(search != prepared_.end());

    return search->second.transaction();
}

/* Used on commit and abort for second phase of 2PL. */
void LockStore::dropLocks(uint64_t transaction_id,
                          const Transaction &transaction,
                          std::unordered_set<uint64_t> &notify_rws) {
    for (auto &write : transaction.getWriteSet()) {
        Debug("[%lu] ReleaseForWrite: %s", transaction_id, write.first.c_str());
        locks_.ReleaseForWrite(write.first, transaction_id, notify_rws);
    }

    for (auto &read : transaction.getReadSet()) {
        Debug("[%lu] ReleaseForRead: %s", transaction_id, read.first.c_str());
        locks_.ReleaseForRead(read.first, transaction_id, notify_rws);
    }
}

int LockStore::getLocks(uint64_t transaction_id,
                        const Transaction &transaction) {
    const Timestamp &start_timestamp = transaction.start_time();
    Debug("start_time: %lu.%lu", start_timestamp.getTimestamp(),
          start_timestamp.getID());
    int ret = REPLY_OK;
    int status = REPLY_OK;
    // get read locks
    for (auto &read : transaction.getReadSet()) {
        status =
            locks_.LockForRead(read.first, transaction_id, start_timestamp);
        Debug("[%lu] LockForRead returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    // get write locks
    for (auto &write : transaction.getWriteSet()) {
        status =
            locks_.LockForWrite(write.first, transaction_id, start_timestamp);
        Debug("[%lu] LockForWrite returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    return ret;
}

}  // namespace strongstore
