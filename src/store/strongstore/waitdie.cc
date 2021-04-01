#include "store/strongstore/waitdie.h"

#include <algorithm>

using namespace std;

namespace strongstore {

WaitDie::WaitDie() {
    readers = 0;
    writers = 0;
}

WaitDie::~WaitDie() {}

WaitDie::Lock::Lock()
    : state_{UNLOCKED},
      min_holder_timestamp_{Timestamp::MAX},
      min_waiter_timestamp_{Timestamp::MAX} {};

void WaitDie::Lock::AddReadWaiter(uint64_t requester,
                                  const Timestamp &start_timestamp,
                                  const Timestamp &waiting_for) {
    std::shared_ptr<Waiter> w = std::make_shared<Waiter>(
        false, requester, start_timestamp, waiting_for);

    waiters_.emplace(requester, w);
    wait_q_.push(requester);
}

void WaitDie::Lock::AddWriteWaiter(uint64_t requester,
                                   const Timestamp &start_timestamp,
                                   const Timestamp &waiting_for) {
    std::shared_ptr<Waiter> w =
        std::make_shared<Waiter>(true, requester, start_timestamp, waiting_for);

    waiters_.emplace(requester, w);
    wait_q_.push(requester);
}

bool WaitDie::Lock::TryReadWait(uint64_t requester,
                                const Timestamp &start_timestamp) {
    for (uint64_t h : holders_) {
        Debug("[%lu] holders: %lu", requester, h);
    }

    if (wait_q_.empty()) {
        Debug("[%lu] wait q empty: %lu <? %lu %d", requester,
              start_timestamp.getTimestamp(),
              min_holder_timestamp_.getTimestamp(),
              start_timestamp < min_holder_timestamp_);
        if (start_timestamp < min_holder_timestamp_) {
            AddReadWaiter(requester, start_timestamp, min_holder_timestamp_);
            return true;
        } else {
            return false;
        }
    }

    ASSERT(waiters_.find(wait_q_.back()) != waiters_.end());
    std::shared_ptr<Waiter> back = waiters_[wait_q_.back()];

    Debug("[%lu] back: %d %lu %lu", requester, back->is_write(),
          back->waiting_for().getTimestamp(),
          back->min_waiter().getTimestamp());

    if (!back->is_write() && start_timestamp < back->waiting_for()) {
        back->add_waiter(requester, start_timestamp);
        waiters_.emplace(requester, back);
        return true;
    } else if (back->is_write() && start_timestamp < back->min_waiter()) {
        AddReadWaiter(requester, start_timestamp, back->min_waiter());
        return true;
    } else {
        return false;
    }

    NOT_REACHABLE();
    return false;
}

int WaitDie::Lock::TryAcquireReadLock(uint64_t requester,
                                      const Timestamp &start_timestamp) {
    // Lock is free
    if (state_ == UNLOCKED) {
        Debug("[%lu] unlocked", requester);
        ASSERT(holders_.size() == 0);
        ASSERT(min_holder_timestamp_ == Timestamp::MAX);

        state_ = LOCKED_FOR_READ;
        holders_.insert(requester);
        min_holder_timestamp_ = start_timestamp;

        return REPLY_OK;
    }

    // I already hold the lock
    if (holders_.find(requester) != holders_.end()) {
        Debug("[%lu] already hold lock", requester);
        if (state_ == LOCKED_FOR_WRITE) {
            state_ = LOCKED_FOR_READ_WRITE;
            Debug("[%lu] upgrade to rw lock", requester);
        }

        return REPLY_OK;
    }

    // There is no write waiting, take read lock
    if (state_ == LOCKED_FOR_READ && !isWriteNext()) {
        holders_.insert(requester);
        min_holder_timestamp_ =
            std::min(min_holder_timestamp_, start_timestamp);

        Debug("[%lu] adding myself as reader: %lu", requester, holders_.size());

        for (uint64_t h : holders_) {
            Debug("[%lu] holders: %lu", requester, h);
        }

        return REPLY_OK;
    }

    // Try wait
    if (TryReadWait(requester, start_timestamp)) {
        Debug("[%lu] Waiting on lock", requester);
        return REPLY_WAIT;
    }

    Debug("[%lu] Die", requester);
    // Die
    return REPLY_FAIL;
}

void WaitDie::Lock::PopWaiter() {
    if (wait_q_.size() == 0) {
        state_ = UNLOCKED;
        min_holder_timestamp_ = Timestamp::MAX;
        return;
    }

    uint64_t h = wait_q_.front();
    wait_q_.pop();
    if (wait_q_.size() == 0) {
        min_waiter_timestamp_ = Timestamp::MAX;
    }

    // TODO: Fix if waiter is removed
    // TODO: Fix for multiple waiters
    ASSERT(waiters_.find(h) != waiters_.end());

    std::shared_ptr<Waiter> w = waiters_[h];

    for (uint64_t v : w->waiters()) {
        Debug("Waiter: %lu", v);
    }
    Debug("Waiter ts: %lu", w->min_waiter().getTimestamp());

    min_holder_timestamp_ = w->min_waiter();

    // TODO: fix for read+write
    if (w->is_write()) {
        state_ = LOCKED_FOR_WRITE;
    } else {
        state_ = LOCKED_FOR_READ;
    }

    holders_ = std::move(w->waiters());
    for (uint64_t t : holders_) {
        waiters_.erase(t);
    }
}

void WaitDie::Lock::ReleaseReadLock(uint64_t holder,
                                    std::unordered_set<uint64_t> &notify_rws) {
    for (uint64_t h : holders_) {
        Debug("[%lu] holders before: %lu", holder, h);
    }

    if (state_ == LOCKED_FOR_READ_WRITE) {
        Debug("[%lu] downgrade to w lock", holder);
        state_ = LOCKED_FOR_WRITE;
        return;
    }

    holders_.erase(holder);

    for (uint64_t h : holders_) {
        Debug("[%lu] holders after: %lu", holder, h);
    }

    std::size_t h = holders_.size();
    if (h == 0 ||  // no holders
        (h == 1 && !wait_q_.empty() && holders_.count(wait_q_.front()) > 1)) {
        // if (!holders_.empty()) {
        //     return;
        // }

        PopWaiter();

        notify_rws.insert(holders_.begin(), holders_.end());
    }
}

void WaitDie::Lock::ReleaseWriteLock(uint64_t holder,
                                     std::unordered_set<uint64_t> &notify_rws) {
    for (uint64_t h : holders_) {
        Debug("[%lu] holders before: %lu", holder, h);
    }

    if (state_ == LOCKED_FOR_READ_WRITE) {
        Debug("[%lu] downgrade to r lock", holder);
        state_ = LOCKED_FOR_READ;
        return;
    }

    holders_.erase(holder);

    for (uint64_t h : holders_) {
        Debug("[%lu] holders after: %lu", holder, h);
    }
    if (!holders_.empty()) {
        return;
    }

    PopWaiter();

    notify_rws.insert(holders_.begin(), holders_.end());
}

bool WaitDie::Lock::TryWriteWait(uint64_t requester,
                                 const Timestamp &start_timestamp) {
    if (wait_q_.empty()) {
        Debug("[%lu] wait q empty: %lu <=? %lu %d", requester,
              start_timestamp.getTimestamp(),
              min_holder_timestamp_.getTimestamp(),
              start_timestamp <= min_holder_timestamp_);
        if (start_timestamp <= min_holder_timestamp_) {
            AddWriteWaiter(requester, start_timestamp, min_holder_timestamp_);
            return true;
        }

        return false;
    }

    ASSERT(waiters_.find(wait_q_.back()) != waiters_.end());
    std::shared_ptr<Waiter> back = waiters_[wait_q_.back()];

    Debug("[%lu] back: %d %lu %lu", requester, back->is_write(),
          back->waiting_for().getTimestamp(),
          back->min_waiter().getTimestamp());

    const Timestamp &min_waiter = back->min_waiter();

    if (start_timestamp <= min_waiter) {
        AddWriteWaiter(requester, start_timestamp, min_waiter);
        return true;
    }

    return false;
}

int WaitDie::Lock::TryAcquireWriteLock(uint64_t requester,
                                       const Timestamp &start_timestamp) {
    // Lock is free
    if (state_ == UNLOCKED) {
        Debug("[%lu] unlocked", requester);
        ASSERT(holders_.size() == 0);
        ASSERT(min_holder_timestamp_ == Timestamp::MAX);

        state_ = LOCKED_FOR_WRITE;
        holders_.insert(requester);
        min_holder_timestamp_ = start_timestamp;

        return REPLY_OK;
    }

    // I already hold the lock
    if (holders_.size() == 1 && holders_.count(requester) > 0) {
        Debug("[%lu] already hold lock", requester);
        if (state_ == LOCKED_FOR_READ) {
            Debug("[%lu] upgrade to rw lock", requester);
            state_ = LOCKED_FOR_READ_WRITE;
        }

        return REPLY_OK;
    }

    // Try wait
    if (TryWriteWait(requester, start_timestamp)) {
        Debug("[%lu] Waiting on lock", requester);
        return REPLY_WAIT;
    }

    Debug("[%lu] die", requester);
    // Die
    return REPLY_FAIL;
}

bool WaitDie::Lock::isWriteNext() {
    if (wait_q_.size() == 0) {
        return false;
    } else {
        return waiters_[wait_q_.front()]->is_write();
    }
}

int WaitDie::LockForRead(const string &lock, uint64_t requester,
                         const Timestamp &start_timestamp) {
    Lock &l = locks[lock];
    Debug("[%lu] Lock for Read: %s", requester, lock.c_str());

    return l.TryAcquireReadLock(requester, start_timestamp);
}

int WaitDie::LockForWrite(const string &lock, uint64_t requester,
                          const Timestamp &start_timestamp) {
    Lock &l = locks[lock];

    Debug("[%lu] Lock for Write: %s", requester, lock.c_str());

    return l.TryAcquireWriteLock(requester, start_timestamp);
}

void WaitDie::ReleaseForRead(const string &lock, uint64_t holder,
                             std::unordered_set<uint64_t> &notify_rws) {
    if (locks.find(lock) == locks.end()) {
        return;
    }

    Lock &l = locks[lock];

    l.ReleaseReadLock(holder, notify_rws);
}

void WaitDie::ReleaseForWrite(const string &lock, uint64_t holder,
                              std::unordered_set<uint64_t> &notify_rws) {
    if (locks.find(lock) == locks.end()) {
        return;
    }

    Lock &l = locks[lock];

    l.ReleaseWriteLock(holder, notify_rws);
}
};  // namespace strongstore
