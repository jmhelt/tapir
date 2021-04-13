#include "store/strongstore/locktable.h"

namespace strongstore {

LockTable::LockTable() {}

LockTable::~LockTable() {}

LockAcquireResult LockTable::ConvertToResult(int status) {
    if (status == REPLY_OK) {
        return {LockStatus::ACQUIRED};
    } else if (status == REPLY_WAIT) {
        return {LockStatus::WAITING};
    } else if (status == REPLY_FAIL) {
        return {LockStatus::FAIL};
    } else {
        NOT_REACHABLE();
    }
}

LockAcquireResult LockTable::AcquireReadLock(uint64_t transaction_id, const Timestamp &ts, const std::string &key) {
    int status = locks_.LockForRead(key, transaction_id, ts);
    return ConvertToResult(status);
}

bool LockTable::HasReadLock(uint64_t transaction_id, const std::string &key) {
    return locks_.HasReadLock(key, transaction_id);
}

LockAcquireResult LockTable::AcquireLocks(uint64_t transaction_id, const Transaction &transaction) {
    const Timestamp &start_ts = transaction.start_time();
    Debug("[%lu] start_time: %lu.%lu", transaction_id, start_ts.getTimestamp(), start_ts.getID());

    int ret = REPLY_OK;

    // get read locks
    for (auto &read : transaction.getReadSet()) {
        int status = locks_.LockForRead(read.first, transaction_id, start_ts);
        Debug("[%lu] LockForRead returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    // get write locks
    for (auto &write : transaction.getWriteSet()) {
        int status = locks_.LockForWrite(write.first, transaction_id, start_ts);
        Debug("[%lu] LockForWrite returned status %d", transaction_id, status);
        if (ret == REPLY_OK && status == REPLY_WAIT) {
            ret = REPLY_WAIT;
        } else if (status == REPLY_FAIL) {
            ret = REPLY_FAIL;
        }
    }

    return ConvertToResult(ret);
}

LockReleaseResult LockTable::ReleaseLocks(uint64_t transaction_id, const Transaction &transaction) {
    LockReleaseResult r;

    for (auto &write : transaction.getWriteSet()) {
        Debug("[%lu] ReleaseForWrite: %s", transaction_id, write.first.c_str());
        locks_.ReleaseForWrite(write.first, transaction_id, r.notify_rws);
    }

    for (auto &read : transaction.getReadSet()) {
        Debug("[%lu] ReleaseForRead: %s", transaction_id, read.first.c_str());
        locks_.ReleaseForRead(read.first, transaction_id, r.notify_rws);
    }

    return r;
}

}  // namespace strongstore
