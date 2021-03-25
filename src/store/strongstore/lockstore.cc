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
    : store_{}, locks_{}, prepared_{}, stats_{}, consistency_{consistency} {}
LockStore::~LockStore() {}

int LockStore::Get(uint64_t transaction_id, const string &key,
                   pair<Timestamp, string> &value) {
    Debug("[%lu] GET %s", transaction_id, BytesToHex(key, 16).c_str());

    // grab the lock (ok, if we already have it)
    if (!locks_.lockForRead(key, transaction_id)) {
        return REPLY_FAIL;
    }

    if (!store_.get(key, value)) {
        Debug("[%lu] Couldn't find key %s", transaction_id,
              BytesToHex(key, 16).c_str());
        // couldn't find the key
        return REPLY_FAIL;
    }

    Debug("[%lu] GET for key %s; return ts %lu.%lu.", transaction_id,
          BytesToHex(key, 16).c_str(), value.first.getTimestamp(),
          value.first.getID());

    return REPLY_OK;
}

int LockStore::ROBegin(uint64_t transaction_id,
                       const std::unordered_set<std::string> &keys,
                       const Timestamp &commit_timestamp,
                       uint64_t &n_conflicting_prepared) {
    n_conflicting_prepared = 0;

    for (auto &p : prepared_) {
        PreparedTransaction &pt = p.second;

        if (consistency_ == Consistency::RSS &&
            commit_timestamp < pt.nonblock_timestamp()) {
            Debug("Not waiting for prepared transaction: %lu < %lu",
                  commit_timestamp.getTimestamp(),
                  pt.nonblock_timestamp().getTimestamp());
            continue;
        }

        const Transaction &transaction = pt.transaction();

        for (auto &w : transaction.getWriteSet()) {
            if (keys.count(w.first) != 0) {
                Debug("%lu conflicts with %lu %lu", transaction_id,
                      pt.transaction_id(), p.first);
                pt.add_waiting_ro(transaction_id);
                n_conflicting_prepared += 1;
                break;
            }
        }
    }

    stats_.IncrementList("n_conflicting_prepared", n_conflicting_prepared);
    if (n_conflicting_prepared == 0) {
        return REPLY_OK;
    } else {
        return REPLY_FAIL;
    }
}

int LockStore::ROGet(uint64_t transaction_id, const string &key,
                     const Timestamp &timestamp,
                     pair<Timestamp, string> &value) {
    Debug("[%lu] RO GET %s", transaction_id, BytesToHex(key, 16).c_str());

    if (!store_.get(key, timestamp, value)) {
        Debug("[%lu] Couldn't find key %s", transaction_id,
              BytesToHex(key, 16).c_str());
        // couldn't find the key
        return REPLY_FAIL;
    }

    Debug("[%lu] RO GET for key %s; return ts %lu.%lu.", transaction_id,
          BytesToHex(key, 16).c_str(), value.first.getTimestamp(),
          value.first.getID());

    return REPLY_OK;
}

int LockStore::Prepare(uint64_t transaction_id, const Transaction &transaction,
                       const Timestamp &nonblock_timestamp) {
    int status = Prepare(transaction_id, transaction);
    if (status == REPLY_OK) {
        Debug("nonblock_timestamp: %lu.%lu", nonblock_timestamp.getTimestamp(),
              nonblock_timestamp.getID());
        prepared_[transaction_id].set_nonblock_timestamp(nonblock_timestamp);
    }

    return status;
}

int LockStore::Prepare(uint64_t transaction_id,
                       const Transaction &transaction) {
    Debug("[%lu] START PREPARE", transaction_id);

    if (prepared_.size() > 100) {
        Warning("Lots of prepared transactions! %lu", prepared_.size());
    }

    if (prepared_.find(transaction_id) != prepared_.end()) {
        Debug("[%lu] Already prepared", transaction_id);
        return REPLY_OK;
    }

    if (getLocks(transaction_id, transaction)) {
        prepared_.emplace(transaction_id,
                          PreparedTransaction{transaction_id, transaction});
        Debug("[%lu] PREPARED TO COMMIT", transaction_id);
        return REPLY_OK;
    } else {
        Debug("[%lu] Could not acquire write locks", transaction_id);
        return REPLY_FAIL;
    }
}

bool LockStore::Commit(uint64_t transaction_id, const Timestamp &timestamp,
                       std::unordered_set<uint64_t> &notify_ros) {
    Debug("[%lu] COMMIT", transaction_id);
    ASSERT(prepared_.find(transaction_id) != prepared_.end());

    const PreparedTransaction &prepared = prepared_[transaction_id];

    const Transaction &transaction = prepared.transaction();

    for (auto &write : transaction.getWriteSet()) {
        store_.put(write.first, write.second, timestamp);
    }

    notify_ros = std::move(prepared.waiting_ros());

    // Drop locks.
    dropLocks(transaction_id, transaction);

    prepared_.erase(transaction_id);

    return !transaction.getWriteSet().empty();
}

void LockStore::Abort(uint64_t transaction_id,
                      std::unordered_set<uint64_t> &notify_ros) {
    Debug("[%lu] ABORT", transaction_id);
    auto search = prepared_.find(transaction_id);
    if (search != prepared_.end()) {
        const PreparedTransaction &prepared = search->second;

        notify_ros = std::move(prepared.waiting_ros());

        // Drop locks.
        dropLocks(transaction_id, prepared.transaction());

        prepared_.erase(search);
    }
}

void LockStore::Load(const string &key, const string &value,
                     const Timestamp &timestamp) {
    store_.put(key, value, timestamp);
}

/* Used on commit and abort for second phase of 2PL. */
void LockStore::dropLocks(uint64_t transaction_id,
                          const Transaction &transaction) {
    for (auto &write : transaction.getWriteSet()) {
        locks_.releaseForWrite(write.first, transaction_id);
    }

    for (auto &read : transaction.getReadSet()) {
        locks_.releaseForRead(read.first, transaction_id);
    }
}

bool LockStore::getLocks(uint64_t transaction_id,
                         const Transaction &transaction) {
    Debug("start_time: %lu.%lu", transaction.get_start_time().getTimestamp(),
          transaction.get_start_time().getID());
    bool ret = true;
    // if we don't have read locks, get read locks
    for (auto &read : transaction.getReadSet()) {
        if (!locks_.lockForRead(read.first, transaction_id)) {
            ret = false;
        }
    }
    for (auto &write : transaction.getWriteSet()) {
        if (!locks_.lockForWrite(write.first, transaction_id)) {
            ret = false;
        }
    }
    return ret;
}

}  // namespace strongstore
