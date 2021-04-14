#include "store/strongstore/woundwait.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>

#include "store/common/timestamp.h"

namespace strongstore {

TEST(WoundWait, BasicReadLock) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForRead("lock", 1, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ww.ReleaseForRead("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(wound.size(), 0);
    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, BasicWriteLock) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(wound.size(), 0);
    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, BasicReadWriteLock) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForRead("lock", 1, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    status = ww.LockForWrite("lock", 1, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ww.ReleaseForRead("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(wound.size(), 0);
    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadLock) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForRead("lock", 1, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    status = ww.LockForRead("lock", 2, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    status = ww.LockForRead("lock", 3, Timestamp(), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ww.ReleaseForRead("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);
    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);
    ww.ReleaseForRead("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(wound.size(), 0);
    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiWriteLockWait) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForWrite("lock", 2, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForWrite("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiWriteLockWound) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForWrite("lock", 2, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForWrite("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadWriteLockWound) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForRead("lock", 1, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForWrite("lock", 2, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    ww.ReleaseForRead("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForWrite("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);
    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadWriteLockWait) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForRead("lock", 1, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForWrite("lock", 2, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(wound.size(), 0);

    ww.ReleaseForRead("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForWrite("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);
    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MergeReadWriteWaiter) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForWrite("lock", 2, Timestamp(0), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForWrite("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);
    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadWaiter) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(3), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(2), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForRead("lock", 3, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify.size(), 2);
    ASSERT_EQ(notify.count(2), 1);
    ASSERT_EQ(notify.count(3), 1);
    notify.clear();

    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify.size(), 0);

    ww.ReleaseForRead("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadWaiterRelease1) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(3), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(2), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForRead("lock", 3, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 0);

    ww.ReleaseForRead("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 0);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MultiReadWaiterRelease2) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(3), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(2), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForRead("lock", 3, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    ww.ReleaseForRead("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 0);

    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(notify.size(), 0);

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

TEST(WoundWait, MergeMultiReadWriteWaiter) {
    WoundWait ww;

    std::unordered_set<uint64_t> wound;
    std::unordered_set<uint64_t> notify;

    int status = ww.LockForWrite("lock", 1, Timestamp(3), wound);
    ASSERT_EQ(status, REPLY_OK);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 0);

    status = ww.LockForRead("lock", 2, Timestamp(2), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForRead("lock", 3, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 1);
    ASSERT_EQ(wound.count(1), 1);
    wound.clear();

    status = ww.LockForWrite("lock", 3, Timestamp(1), wound);
    ASSERT_EQ(status, REPLY_WAIT);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

    ASSERT_EQ(wound.size(), 2);
    ASSERT_EQ(wound.count(1), 1);
    ASSERT_EQ(wound.count(2), 1);
    wound.clear();

    ww.ReleaseForWrite("lock", 1, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(2), 1);
    notify.clear();

    ww.ReleaseForRead("lock", 2, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

    ASSERT_EQ(notify.size(), 1);
    ASSERT_EQ(notify.count(3), 1);
    notify.clear();

    ww.ReleaseForRead("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);
    ww.ReleaseForWrite("lock", 3, notify);
    ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

    ASSERT_EQ(notify.size(), 0);
}

// TEST(WoundWait, MergeWriteReadWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForWrite("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForWrite("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForRead("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ww.ReleaseForWrite("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(2), 1);
//     notify.clear();

//     ww.ReleaseForRead("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);
//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseReadWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForWrite("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForRead("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ww.ReleaseForRead("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForWrite("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseWriteWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForRead("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseMultiWriteWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock", 1, Timestamp(2));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 2, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 3, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForRead("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(3), 1);
//     notify.clear();

//     ww.ReleaseForWrite("lock", 3, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseReadWriteWaiter1) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForWrite("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForRead("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForWrite("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ww.ReleaseForRead("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);
//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForWrite("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseReadWriteWaiter2) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForWrite("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForRead("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForWrite("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ww.ReleaseForRead("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForWrite("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(2), 1);
//     notify.clear();

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseReadWriteWaiter3) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForWrite("lock", 1, Timestamp(1));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForRead("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     status = ww.LockForWrite("lock", 2, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForWrite("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(2), 1);
//     notify.clear();

//     ww.ReleaseForRead("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, Release2WriteWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock", 1, Timestamp(2));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 2, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     status = ww.LockForWrite("lock", 3, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForRead("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(3), 1);
//     notify.clear();

//     ww.ReleaseForWrite("lock", 3, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, Release2WriteReadWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock", 1, Timestamp(3));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 2, Timestamp(2));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 3, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForWrite("lock", 3, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     status = ww.LockForRead("lock", 4, Timestamp(0));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForRead("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(2), 1);
//     notify.clear();

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(4), 1);
//     notify.clear();

//     ww.ReleaseForRead("lock", 4, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, ReleaseWriteReadWaiter) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock", 1, Timestamp(2));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock", 2, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForWrite("lock", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     status = ww.LockForRead("lock", 3, Timestamp(0));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ww.ReleaseForRead("lock", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), LOCKED_FOR_READ);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForRead("lock", 3, notify);
//     ASSERT_EQ(ww.GetLockState("lock"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

// TEST(WoundWait, WaitTwoLocks) {
//     WoundWait ww;

//     std::unordered_set<uint64_t> notify;

//     int status = ww.LockForRead("lock1", 1, Timestamp(2));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock1"), LOCKED_FOR_READ);

//     status = ww.LockForRead("lock2", 1, Timestamp(2));
//     ASSERT_EQ(status, REPLY_OK);
//     ASSERT_EQ(ww.GetLockState("lock2"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock1", 2, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock1"), LOCKED_FOR_READ);

//     status = ww.LockForWrite("lock2", 2, Timestamp(1));
//     ASSERT_EQ(status, REPLY_WAIT);
//     ASSERT_EQ(ww.GetLockState("lock2"), LOCKED_FOR_READ);

//     ww.ReleaseForRead("lock1", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock1"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForRead("lock2", 1, notify);
//     ASSERT_EQ(ww.GetLockState("lock2"), LOCKED_FOR_WRITE);

//     ASSERT_EQ(notify.size(), 1);
//     ASSERT_EQ(notify.count(2), 1);
//     notify.clear();

//     ww.ReleaseForWrite("lock1", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock1"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);

//     ww.ReleaseForWrite("lock2", 2, notify);
//     ASSERT_EQ(ww.GetLockState("lock2"), UNLOCKED);

//     ASSERT_EQ(notify.size(), 0);
// }

};  // namespace strongstore
