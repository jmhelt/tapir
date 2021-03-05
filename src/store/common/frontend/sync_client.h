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
    virtual void Begin(uint32_t timeout);

    // Get the value corresponding to key.
    virtual void Get(const std::string &key, std::string &value,
                     uint32_t timeout);

    // Get value without waiting.
    void Get(const std::string &key, uint32_t timeout);

    // Set the value for the given key.
    virtual void Put(const std::string &key, const std::string &value,
                     uint32_t timeout);

    // Commit all Get(s) and Put(s) since Begin().
    virtual transaction_status_t Commit(uint32_t timeout);

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(uint32_t timeout);

   private:
    Client *client;
};

#endif /* _SYNC_CLIENT_API_H_ */
