// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _SYNC_CLIENT_API_H_
#define _SYNC_CLIENT_API_H_

#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"

class SyncClient {
   public:
    SyncClient(Client *client);
    virtual ~SyncClient();

    // Begin a transaction.
    void Begin(bool is_retry, uint32_t timeout);

    // Get the value corresponding to key.
    int Get(const std::string &key, std::string &value, uint32_t timeout);

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    int GetForUpdate(const std::string &key, std::string &value, uint32_t timeout);

    // Get value without waiting.
    void Get(const std::string &key, uint32_t timeout);

    // Wait for outstanding Gets to finish in FIFO order.
    void Wait(std::vector<std::string> &values);

    // Set the value for the given key.
    virtual void Put(const std::string &key, const std::string &value,
                     uint32_t timeout);

    // Commit all Get(s) and Put(s) since Begin().
    virtual transaction_status_t Commit(uint32_t timeout);

    // Commit all Get(s) and Put(s) since Begin().
    virtual transaction_status_t ROCommit(
        const std::unordered_set<std::string> &keys, uint32_t timeout);

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(uint32_t timeout);

   private:
    void GetCallback(Promise *promise, int status, const std::string &key,
                     const std::string &value, Timestamp ts);
    void GetTimeoutCallback(Promise *promise, int status,
                            const std::string &key);
    void PutCallback(Promise *promise, int status, const std::string &key,
                     const std::string &value);
    void PutTimeoutCallback(Promise *promise, int status,
                            const std::string &key, const std::string &value);
    void CommitCallback(Promise *promise, transaction_status_t status);
    void CommitTimeoutCallback(Promise *promise);
    void AbortCallback(Promise *promise);
    void AbortTimeoutCallback(Promise *promise);

    std::vector<Promise *> getPromises;

    Client *client;
};

#endif /* _SYNC_CLIENT_API_H_ */
