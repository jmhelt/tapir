#ifndef _STRONG_TRANSACTION_STORE_H_
#define _STRONG_TRANSACTION_STORE_H_

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"

namespace strongstore {

enum TransactionStatus {
    READING,
    READ_WAIT,
    PREPARING,
    WAIT_PARTICIPANTS,
    PREPARE_WAIT,
    PREPARED,
    COMMITTED,
    ABORTED
};

struct TransactionFinishResult {
    std::unordered_set<uint64_t> notify_ros;
};

class TransactionStore {
   public:
    TransactionStore(const TrueTime &tt, Consistency consistency);
    ~TransactionStore();

    TransactionStatus GetTransactionStatus(uint64_t transaction_id);
    const Transaction &GetTransaction(uint64_t transaction_id);
    const Timestamp &GetRWCommitTimestamp(uint64_t transaction_id);
    const Timestamp &GetStartTimestamp(uint64_t transaction_id);
    const std::unordered_set<int> &GetParticipants(uint64_t transaction_id);
    const Timestamp &GetNonBlockTimestamp(uint64_t transaction_id);

    const Timestamp &GetROCommitTimestamp(uint64_t transaction_id);
    const std::unordered_set<std::string> &GetROKeys(uint64_t transaction_id);

    uint64_t StartRO(uint64_t transaction_id,
                     const std::unordered_set<std::string> &keys,
                     const Timestamp &min_ts,
                     const Timestamp &commit_ts);
    void ContinueRO(uint64_t transaction_id);
    void CommitRO(uint64_t transaction_id);

    void StartGet(uint64_t transaction_id, const std::string &key);
    void FinishGet(uint64_t transaction_id, const std::string &key);
    void AbortGet(uint64_t transaction_id, const std::string &key);
    void PauseGet(uint64_t transaction_id, const std::string &key);
    void ContinueGet(uint64_t transaction_id, const std::string &key);

    TransactionStatus StartCoordinatorPrepare(uint64_t transaction_id, const Timestamp &start_ts,
                                              int coordinator, const std::unordered_set<int> participants,
                                              const Transaction &transaction,
                                              const Timestamp &nonblock_ts);
    void FinishCoordinatorPrepare(uint64_t transaction_id, const Timestamp &prepare_ts);
    void AbortPrepare(uint64_t transaction_id);
    void PausePrepare(uint64_t transaction_id);
    void ContinuePrepare(uint64_t transaction_id);

    TransactionFinishResult Commit(uint64_t transaction_id);
    TransactionFinishResult Abort(uint64_t transaction_id);

    Stats &GetStats() { return stats_; };

   private:
    class PendingRWTransaction {
       public:
        PendingRWTransaction() : status_{READING} {}
        ~PendingRWTransaction() {}

        TransactionStatus status() const { return status_; }
        void set_status(TransactionStatus s) { status_ = s; }

        const Timestamp &start_ts() const { return start_ts_; }
        const Timestamp &nonblock_ts() const { return nonblock_ts_; }
        const Timestamp &prepare_ts() const { return prepare_ts_; }
        const Timestamp &commit_ts() const { return commit_ts_; }
        const Transaction &transaction() const { return transaction_; }

        const std::unordered_set<int> &participants() const { return participants_; }

        const std::unordered_set<uint64_t> &waiting_ros() const { return waiting_ros_; }
        void add_waiting_ro(uint64_t transaction_id) {
            waiting_ros_.insert(transaction_id);
        }

        void AddReadSet(const std::string &key);

        void StartCoordinatorPrepare(const Timestamp &start_ts, int coordinator,
                                     const std::unordered_set<int> participants,
                                     const Transaction &transaction,
                                     const Timestamp &nonblock_ts);

        void FinishCoordinatorPrepare(const Timestamp &prepare_ts);

       private:
        Transaction transaction_;
        std::unordered_set<int> participants_;
        std::unordered_set<uint64_t> waiting_ros_;
        Timestamp start_ts_;
        Timestamp nonblock_ts_;
        Timestamp prepare_ts_;
        Timestamp commit_ts_;
        int coordinator_;
        TransactionStatus status_;
    };

    class PendingROTransaction {
       public:
        PendingROTransaction() : status_{PREPARING} {}
        ~PendingROTransaction() {}

        TransactionStatus status() const { return status_; }
        void set_status(TransactionStatus s) { status_ = s; }

        const Timestamp &commit_ts() const { return commit_ts_; }
        const std::unordered_set<std::string> &keys() const { return keys_; }

        void StartRO(const std::unordered_set<std::string> &keys,
                     const Timestamp &min_ts,
                     const Timestamp &commit_ts);

       private:
        std::unordered_set<std::string> keys_;
        Timestamp min_ts_;
        Timestamp commit_ts_;
        TransactionStatus status_;
    };

    const TrueTime &tt_;
    std::unordered_map<uint64_t, PendingRWTransaction> pending_rw_;
    std::unordered_map<uint64_t, PendingROTransaction> pending_ro_;
    std::unordered_set<uint64_t> committed_;
    std::unordered_set<uint64_t> aborted_;
    Stats stats_;
    Consistency consistency_;
};

}  // namespace strongstore

#endif /* _STRONG_TRANSACTION_STORE_H_ */
