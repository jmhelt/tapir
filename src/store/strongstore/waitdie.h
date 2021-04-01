#ifndef _STRONG_WAIT_DIE_H_
#define _STRONG_WAIT_DIE_H_

#include <sys/time.h>

#include <map>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace strongstore {

enum LockState {
    UNLOCKED,
    LOCKED_FOR_READ,
    LOCKED_FOR_WRITE,
    LOCKED_FOR_READ_WRITE
};

class WaitDie {
   public:
    WaitDie();
    ~WaitDie();

    const LockState GetLockState(const std::string &lock) const;

    int LockForRead(const std::string &lock, uint64_t requester,
                    const Timestamp &start_timestamp);
    int LockForWrite(const std::string &lock, uint64_t requester,
                     const Timestamp &start_timestamp);
    void ReleaseForRead(const std::string &lock, uint64_t holder,
                        std::unordered_set<uint64_t> &notify_rws);
    void ReleaseForWrite(const std::string &lock, uint64_t holder,
                         std::unordered_set<uint64_t> &notify_rws);

   private:
    class Waiter {
       public:
        Waiter() : write_{false} {}
        Waiter(bool r, bool w, uint64_t waiter,
               const Timestamp &start_timestamp, const Timestamp &waiting_for)
            : waiters_{},
              min_waiter_{Timestamp::MAX},
              waiting_for_{waiting_for},
              write_{w},
              read_{r} {
            add_waiter(waiter, start_timestamp);
        }

        bool isread() const { return read_; }
        void set_read(bool r) { read_ = r; }

        bool iswrite() const { return write_; }
        void set_write(bool w) { write_ = w; }

        const Timestamp &min_waiter() const { return min_waiter_; }
        const Timestamp &waiting_for() const { return waiting_for_; }

        void add_waiter(uint64_t w, const Timestamp &ts) {
            waiters_.insert(w);
            min_waiter_ = std::min(min_waiter_, ts);
        }

        void remove_waiter(uint64_t w) { waiters_.erase(w); }

        const std::unordered_set<uint64_t> &waiters() const { return waiters_; }

       private:
        std::unordered_set<uint64_t> waiters_;
        Timestamp min_waiter_;
        Timestamp waiting_for_;
        bool write_;
        bool read_;
    };

    class Lock {
       public:
        Lock();

        int TryAcquireReadLock(uint64_t requester,
                               const Timestamp &start_timestamp);
        int TryAcquireWriteLock(uint64_t requester,
                                const Timestamp &start_timestamp);

        void ReleaseReadLock(uint64_t holder,
                             std::unordered_set<uint64_t> &notify_rws);
        void ReleaseWriteLock(uint64_t holder,
                              std::unordered_set<uint64_t> &notify_rws);

        const LockState state() const { return state_; }

       private:
        LockState state_;
        std::unordered_set<uint64_t> holders_;
        std::queue<uint64_t> wait_q_;
        std::unordered_map<uint64_t, std::shared_ptr<Waiter>> waiters_;
        Timestamp min_holder_timestamp_;
        Timestamp min_waiter_timestamp_;

        bool isWriteNext();

        bool TryReadWait(uint64_t requester, const Timestamp &start_timestamp);
        bool TryWriteWait(uint64_t requester, const Timestamp &start_timestamp);

        void AddReadWaiter(uint64_t requester, const Timestamp &start_timestamp,
                           const Timestamp &waiting_for);
        void AddWriteWaiter(uint64_t requester,
                            const Timestamp &start_timestamp,
                            const Timestamp &waiting_for);
        void AddReadWriteWaiter(uint64_t requester,
                                const Timestamp &start_timestamp,
                                const Timestamp &waiting_for);

        bool PopWaiter();
    };

    /* Global store which keep key -> (timestamp, value) list. */
    std::unordered_map<std::string, Lock> locks;

    uint64_t readers;
    uint64_t writers;
};

};  // namespace strongstore

#endif /* _STRONG_WAIT_DIE_H_ */
