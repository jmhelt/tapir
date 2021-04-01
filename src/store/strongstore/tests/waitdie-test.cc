#include "store/strongstore/waitdie.h"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "store/common/timestamp.h"

namespace strongstore {

TEST(WaitDie, BasicReadLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    wd.ReleaseForRead("lock", 1, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, BasicWriteLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    wd.ReleaseForWrite("lock", 1, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, BasicReadWriteLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForWrite("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    wd.ReleaseForRead("lock", 1, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadLock) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForRead("lock", 2, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForRead("lock", 3, Timestamp());
    ASSERT_EQ(status, REPLY_OK);

    wd.ReleaseForRead("lock", 1, notify_rws);
    wd.ReleaseForRead("lock", 2, notify_rws);
    wd.ReleaseForRead("lock", 3, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiWriteLockWait) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);

    wd.ReleaseForWrite("lock", 1, notify_rws);
    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);

    notify_rws.clear();
    wd.ReleaseForRead("lock", 2, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiWriteLockDie) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForWrite("lock", 1, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForWrite("lock", 2, Timestamp(1));
    ASSERT_EQ(status, REPLY_FAIL);

    wd.ReleaseForWrite("lock", 1, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

TEST(WaitDie, MultiReadWriteLockWait) {
    WaitDie wd;

    std::unordered_set<uint64_t> notify_rws;

    int status = wd.LockForRead("lock", 1, Timestamp(1));
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForRead("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_OK);

    status = wd.LockForWrite("lock", 2, Timestamp(0));
    ASSERT_EQ(status, REPLY_WAIT);

    wd.ReleaseForRead("lock", 1, notify_rws);
    ASSERT_EQ(notify_rws.size(), 1);
    ASSERT_EQ(notify_rws.count(2), 1);

    notify_rws.clear();
    wd.ReleaseForWrite("lock", 2, notify_rws);
    wd.ReleaseForRead("lock", 2, notify_rws);

    ASSERT_EQ(notify_rws.size(), 0);
}

};  // namespace strongstore
