// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/lockstore.h:
 *    Single-node Key-value store with support for 2PC locking-based
 *    transactions using S2PL
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

#ifndef _STRONG_LOCK_STORE_H_
#define _STRONG_LOCK_STORE_H_

#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/versionstore.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"
#include "store/strongstore/common.h"
#include "store/strongstore/waitdie.h"

namespace strongstore {

class LockStore {
   public:
    LockStore(Consistency consistency);
    ~LockStore();

    int Get(uint64_t transaction_id, const Timestamp &start_timestamp,
            const std::string &key, std::pair<Timestamp, std::string> &value);

    int ROBegin(uint64_t transaction_id,
                const std::unordered_set<std::string> &keys,
                const Timestamp &commit_timestamp,
                const Timestamp &min_timestamp,
                uint64_t &n_conflicting_prepared);

    int ROGet(uint64_t transaction_id, const std::string &key,
              const Timestamp &timestamp,
              std::pair<Timestamp, std::string> &value);

    int Prepare(uint64_t transaction_id, const Transaction &transaction,
                const Timestamp &prepare_timestamp);
    int Prepare(uint64_t transaction_id, const Transaction &transaction,
                const Timestamp &prepare_timestamp,
                const Timestamp &nonblock_timestamp);

    int ContinuePrepare(uint64_t transaction_id,
                        const Timestamp &prepare_timestamp,
                        std::unordered_set<uint64_t> &notify_rws);

    bool Commit(uint64_t transaction_id, const Timestamp &timestamp,
                std::unordered_set<uint64_t> &notify_rws,
                std::unordered_set<uint64_t> &notify_ros);

    void Abort(uint64_t transaction_id,
               std::unordered_set<uint64_t> &notify_rws,
               std::unordered_set<uint64_t> &notify_ros);
    void ReleaseLocks(uint64_t transaction_id, const Transaction &transaction,
                      std::unordered_set<uint64_t> &notify_rws);

    void Load(const std::string &key, const std::string &value,
              const Timestamp &timestamp);

    Stats &GetStats() { return stats_; };

    const Transaction &GetPreparedTransaction(uint64_t transaction_id) const;

   private:
    class PreparedTransaction {
       public:
        PreparedTransaction()
            : transaction_{},
              waiting_ros_{},
              prepare_timestamp_{},
              nonblock_timestamp_{},
              transaction_id_{} {}

        PreparedTransaction(uint64_t transaction_id,
                            const Transaction &transaction)
            : transaction_{transaction},
              waiting_ros_{},
              prepare_timestamp_{},
              nonblock_timestamp_{},
              transaction_id_{transaction_id} {}

        const Transaction &transaction() const { return transaction_; }

        const uint64_t transaction_id() const { return transaction_id_; }

        const Timestamp &prepare_timestamp() const {
            return prepare_timestamp_;
        }
        void set_prepare_timestamp(const Timestamp &prepare_timestamp) {
            prepare_timestamp_ = prepare_timestamp;
        }

        const Timestamp &nonblock_timestamp() const {
            return nonblock_timestamp_;
        }
        void set_nonblock_timestamp(const Timestamp &nonblock_timestamp) {
            nonblock_timestamp_ = nonblock_timestamp;
        }

        const std::unordered_set<uint64_t> &waiting_ros() const {
            return waiting_ros_;
        }

        void add_waiting_ro(uint64_t transaction_id) {
            waiting_ros_.insert(transaction_id);
        }

       private:
        Transaction transaction_;
        std::unordered_set<uint64_t> waiting_ros_;
        Timestamp prepare_timestamp_;
        Timestamp nonblock_timestamp_;
        uint64_t transaction_id_;
    };

    // Data store.
    VersionedKVStore<Timestamp, std::string> store_;

    // Locks manager.
    WaitDie locks_;

    std::unordered_map<uint64_t, PreparedTransaction> prepared_;
    std::unordered_map<uint64_t, PreparedTransaction> waiting_;

    Stats stats_;

    Consistency consistency_;

    int getLocks(uint64_t transaction_id, const Transaction &txn);
    void dropLocks(uint64_t transaction_id, const Transaction &txn,
                   std::unordered_set<uint64_t> &notify_rws);
};

}  // namespace strongstore

#endif /* _STRONG_LOCK_STORE_H_ */
