#include "store/strongstore/woundwait.h"

#include <algorithm>

namespace strongstore {

WoundWait::WoundWait() {}

WoundWait::~WoundWait() {}

WoundWait::Lock::Lock() : state_{UNLOCKED} {};

void WoundWait::Lock::AddReadWaiter(uint64_t requester, const Timestamp &ts) {
    std::shared_ptr<Waiter> w = std::make_shared<Waiter>(true, false, requester, ts);

    waiters_.emplace(requester, w);
    wait_q_.push_back(requester);
}

void WoundWait::Lock::AddWriteWaiter(uint64_t requester, const Timestamp &ts) {
    std::shared_ptr<Waiter> w = std::make_shared<Waiter>(false, true, requester, ts);

    waiters_.emplace(requester, w);
    wait_q_.push_back(requester);
}

void WoundWait::Lock::AddReadWriteWaiter(uint64_t requester, const Timestamp &ts) {
    std::shared_ptr<Waiter> w = std::make_shared<Waiter>(true, true, requester, ts);

    waiters_.emplace(requester, w);
    wait_q_.push_back(requester);
}

bool WoundWait::Lock::ReadWait(uint64_t requester, const Timestamp &ts,
                               std::unordered_set<uint64_t> &wound) {
    auto search = waiters_.find(requester);
    if (search != waiters_.end()) {  // I already have a waiter
        std::shared_ptr<Waiter> w = search->second;
        bool isread = w->isread();
        bool iswrite = w->iswrite();

        // Read is already waiting
        if (isread) {
            return true;
        } else if (iswrite) {
            // Upgrade waiting write to read-write
            w->set_read(true);
            return true;
        } else {
            NOT_REACHABLE();
        }
    }

    // Add waiter
    if (wait_q_.size() > 0) {
        // Try merging with readers at end of queue
        uint64_t b = wait_q_.back();
        auto search = waiters_.find(b);
        if (search != waiters_.end() && !search->second->iswrite()) {
            search->second->add_waiter(requester, ts);
            waiters_.emplace(requester, search->second);
        } else {
            AddReadWaiter(requester, ts);
        }
    } else {
        AddReadWaiter(requester, ts);
    }

    // Wound other waiters
    for (auto it = wait_q_.begin(); it != wait_q_.end();) {
        uint64_t h = *it;

        // Already in wound set
        if (wound.count(h) > 0) {
            ++it;
            continue;
        }

        // Waiter already released lock
        auto search = waiters_.find(h);
        if (search == waiters_.end()) {
            it = wait_q_.erase(it);
            continue;
        }

        std::shared_ptr<Waiter> waiter = search->second;

        // No need to wound other readers
        if (waiter->waiters().count(requester) > 0) {
            ++it;
            continue;
        }

        for (auto w : waiter->waiters()) {
            if (ts < w.second) {
                wound.insert(w.first);
            }
        }

        ++it;
    }

    // Wound holders
    for (auto w : holders_) {
        if (w.first != requester && ts < w.second) {
            wound.insert(w.first);
        }
    }

    return true;
}

int WoundWait::Lock::TryAcquireReadLock(uint64_t requester, const Timestamp &ts,
                                        std::unordered_set<uint64_t> &wound) {
    // Lock is free
    if (state_ == UNLOCKED) {
        Debug("[%lu] unlocked", requester);
        ASSERT(holders_.size() == 0);

        state_ = LOCKED_FOR_READ;
        holders_[requester] = ts;

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
        holders_[requester] = ts;

        Debug("[%lu] adding myself as reader: %lu", requester, holders_.size());

        return REPLY_OK;
    }

    // Wait (and possibly wound)
    Debug("[%lu] Waiting on lock", requester);
    ReadWait(requester, ts, wound);
    return REPLY_WAIT;
}

bool WoundWait::Lock::PopWaiter() {
    bool modified = false;

    for (std::size_t nh = holders_.size(); !wait_q_.empty() && nh < 2; nh = holders_.size()) {
        uint64_t next = wait_q_.front();
        auto search = waiters_.find(next);
        if (search == waiters_.end()) {
            wait_q_.pop_front();
            continue;
        }

        if (nh == 1 && holders_.count(next) == 0) {
            break;
        }

        wait_q_.pop_front();

        std::shared_ptr<Waiter> w = search->second;

        bool isread = w->isread();
        bool iswrite = w->iswrite();
        if (iswrite && holders_.count(next) > 0) {
            state_ = LOCKED_FOR_READ_WRITE;
        } else if (isread && iswrite) {
            state_ = LOCKED_FOR_READ_WRITE;
        } else if (iswrite) {
            state_ = LOCKED_FOR_WRITE;
        } else {
            state_ = LOCKED_FOR_READ;
        }

        holders_ = std::move(w->waiters());
        for (auto h : holders_) {
            waiters_.erase(h.first);
        }
        modified = true;
    }

    if (holders_.size() == 0) {
        state_ = UNLOCKED;
    }

    return modified;
}

void WoundWait::Lock::ReleaseReadLock(uint64_t holder, std::unordered_set<uint64_t> &notify) {
    // Clean up waiter
    auto search = waiters_.find(holder);
    if (search != waiters_.end()) {
        std::shared_ptr<Waiter> w = search->second;

        if (w->iswrite()) {
            w->set_read(false);
        } else if (w->waiters().size() > 1) {
            w->remove_waiter(holder);
            waiters_.erase(search);
        } else {
            waiters_.erase(search);
        }
    }

    Debug("holders before:");
    for (auto h : holders_) {
        Debug("%lu", h.first);
    }

    if (holders_.count(holder) > 0 &&
        (state_ == LOCKED_FOR_READ || state_ == LOCKED_FOR_READ_WRITE)) {
        if (state_ == LOCKED_FOR_READ_WRITE) {
            Debug("[%lu] downgrade to w lock", holder);
            state_ = LOCKED_FOR_WRITE;
            return;
        }

        holders_.erase(holder);

        bool popped = PopWaiter();
        if (popped) {
            for (auto h : holders_) {
                notify.insert(h.first);
            }
        }
    }

    Debug("holders after:");
    for (auto h : holders_) {
        Debug("%lu", h.first);
    }
}

void WoundWait::Lock::ReleaseWriteLock(uint64_t holder, std::unordered_set<uint64_t> &notify) {
    // Clean up waiter
    auto search = waiters_.find(holder);
    if (search != waiters_.end()) {
        std::shared_ptr<Waiter> w = search->second;

        if (w->isread()) {
            w->set_write(false);
        } else {
            waiters_.erase(search);
        }
    }

    Debug("holders before:");
    for (auto h : holders_) {
        Debug("%lu", h.first);
    }

    if (holders_.count(holder) > 0 &&
        (state_ == LOCKED_FOR_WRITE || state_ == LOCKED_FOR_READ_WRITE)) {
        if (state_ == LOCKED_FOR_READ_WRITE) {
            Debug("[%lu] downgrade to r lock", holder);
            state_ = LOCKED_FOR_READ;
            return;
        }

        holders_.erase(holder);

        bool popped = PopWaiter();
        if (popped) {
            for (auto h : holders_) {
                notify.insert(h.first);
            }
        }
    }

    Debug("holders after:");
    for (auto h : holders_) {
        Debug("%lu", h.first);
    }
}

bool WoundWait::Lock::WriteWait(uint64_t requester, const Timestamp &ts,
                                std::unordered_set<uint64_t> &wound) {
    bool rw = false;
    auto search = waiters_.find(requester);
    if (search != waiters_.end()) {  // I already have a waiter
        std::shared_ptr<Waiter> w = search->second;
        bool isread = w->isread();
        bool iswrite = w->iswrite();

        // Write is already waiting
        if (iswrite) {
            return true;
        } else if (isread && w->waiters().size() == 1) {
            // Upgrade waiting read to read-write
            w->set_write(true);
            return true;
        } else if (isread) {
            Debug("wait as read-write");
            // Wait as read-write
            w->remove_waiter(requester);
            waiters_.erase(search);
            rw = true;
        } else {
            NOT_REACHABLE();
        }
    }

    // Wound other waiters
    for (auto it = wait_q_.begin(); it != wait_q_.end();) {
        uint64_t h = *it;

        // Already in wound set
        if (wound.count(h) > 0) {
            ++it;
            continue;
        }

        // Waiter already released lock
        auto search = waiters_.find(h);
        if (search == waiters_.end()) {
            it = wait_q_.erase(it);
            continue;
        }

        std::shared_ptr<Waiter> waiter = search->second;

        for (auto w : waiter->waiters()) {
            if (w.first != requester && ts < w.second) {
                wound.insert(w.first);
            }
        }

        ++it;
    }

    // Wound holders
    for (auto w : holders_) {
        if (w.first != requester && ts < w.second) {
            wound.insert(w.first);
        }
    }

    // Add waiter
    if (rw) {
        AddReadWriteWaiter(requester, ts);
    } else {
        AddWriteWaiter(requester, ts);
    }

    return true;
}

int WoundWait::Lock::TryAcquireWriteLock(uint64_t requester, const Timestamp &ts,
                                         std::unordered_set<uint64_t> &wound) {
    // Lock is free
    if (state_ == UNLOCKED) {
        Debug("[%lu] unlocked", requester);
        ASSERT(holders_.size() == 0);

        state_ = LOCKED_FOR_WRITE;
        holders_[requester] = ts;

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

    // Wait (and possibly wound)
    Debug("[%lu] Waiting on lock", requester);
    WriteWait(requester, ts, wound);
    return REPLY_WAIT;
}

bool WoundWait::Lock::isWriteNext() {
    while (!wait_q_.empty()) {
        auto search = waiters_.find(wait_q_.front());
        if (search == waiters_.end()) {
            wait_q_.pop_front();
            continue;
        }

        return search->second->iswrite();
    }

    return false;
}

LockState WoundWait::GetLockState(const std::string &lock) const {
    auto search = locks_.find(lock);
    if (search == locks_.end()) {
        return UNLOCKED;
    }

    return search->second.state();
}

bool WoundWait::HasReadLock(const std::string &lock, uint64_t requester) const {
    auto search = locks_.find(lock);
    if (search == locks_.end()) {
        return false;
    }

    const Lock &l = search->second;
    if (l.state() != LOCKED_FOR_READ && l.state() != LOCKED_FOR_READ_WRITE) {
        return false;
    }

    return l.holders().count(requester) > 0;
}

int WoundWait::LockForRead(const std::string &lock, uint64_t requester,
                           const Timestamp &ts,
                           std::unordered_set<uint64_t> &wound) {
    Lock &l = locks_[lock];
    Debug("[%lu] Lock for Read: %s", requester, lock.c_str());

    int ret = l.TryAcquireReadLock(requester, ts, wound);
    if (ret == REPLY_WAIT) {
        waiting_[requester].insert(lock);
    }

    return ret;
}

int WoundWait::LockForWrite(const std::string &lock, uint64_t requester,
                            const Timestamp &ts,
                            std::unordered_set<uint64_t> &wound) {
    Lock &l = locks_[lock];

    Debug("[%lu] Lock for Write: %s", requester, lock.c_str());

    int ret = l.TryAcquireWriteLock(requester, ts, wound);
    if (ret == REPLY_WAIT) {
        waiting_[requester].insert(lock);
    }

    return ret;
}

bool WoundWait::Notify(const std::string &lock, uint64_t waiter) {
    waiting_[waiter].erase(lock);
    if (waiting_[waiter].size() == 0) {
        waiting_.erase(waiter);
        return true;
    }

    return false;
}

void WoundWait::Notify(const std::string &lock, std::unordered_set<uint64_t> &notify) {
    for (auto it = notify.begin(); it != notify.end();) {
        if (Notify(lock, *it)) {
            ++it;
        } else {
            it = notify.erase(it);
        }
    }
}

void WoundWait::ReleaseForRead(const std::string &lock, uint64_t holder,
                               std::unordered_set<uint64_t> &notify) {
    if (locks_.find(lock) == locks_.end()) {
        return;
    }

    Lock &l = locks_[lock];

    l.ReleaseReadLock(holder, notify);

    Notify(lock, holder);
    Notify(lock, notify);
}

void WoundWait::ReleaseForWrite(const std::string &lock, uint64_t holder,
                                std::unordered_set<uint64_t> &notify) {
    if (locks_.find(lock) == locks_.end()) {
        return;
    }

    Lock &l = locks_[lock];

    l.ReleaseWriteLock(holder, notify);

    Notify(lock, holder);
    Notify(lock, notify);
}
};  // namespace strongstore
