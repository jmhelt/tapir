// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _CLIENT_API_H_
#define _CLIENT_API_H_

#include <functional>
#include <string>
#include <vector>

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"

enum transaction_status_t {
    COMMITTED = 0,
    ABORTED_USER,
    ABORTED_SYSTEM,
    ABORTED_MAX_RETRIES
};

typedef std::function<void(uint64_t)> begin_callback;
typedef std::function<void()> begin_timeout_callback;

typedef std::function<void(int, const std::string &, const std::string &,
                           Timestamp)>
    get_callback;
typedef std::function<void(int, const std::string &)> get_timeout_callback;

typedef std::function<void(int, const std::string &, const std::string &)>
    put_callback;
typedef std::function<void(int, const std::string &, const std::string &)>
    put_timeout_callback;

typedef std::function<void(transaction_status_t)> commit_callback;
typedef std::function<void()> commit_timeout_callback;

typedef std::function<void()> abort_callback;
typedef std::function<void()> abort_timeout_callback;

class Stats;

class Client {
   public:
    Client() { _Latency_Init(&clientLat, "client_lat"); }
    virtual ~Client() {}

    // Begin a transaction.
    virtual void Begin(bool is_retry, begin_callback bcb,
                       begin_timeout_callback btcb, uint32_t timeout) = 0;

    // Get the value corresponding to key.
    virtual void Get(const std::string &key, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) = 0;

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    virtual void GetForUpdate(const std::string &key, get_callback gcb,
                              get_timeout_callback gtcb, uint32_t timeout) {
        Get(key, gcb, gtcb, timeout);
    }

    // Set the value for the given key.
    virtual void Put(const std::string &key, const std::string &value,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout) = 0;

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
                        uint32_t timeout) = 0;

    virtual void ROCommit(const std::unordered_set<std::string> &keys,
                          commit_callback ccb, commit_timeout_callback ctcb,
                          uint32_t timeout) {
        Panic("Unimplemented ROCommit!");
    }

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
                       uint32_t timeout) = 0;

    inline Stats &GetStats() { return stats; }

   protected:
    void StartRecLatency();
    void EndRecLatency(const std::string &str);
    Stats stats;

   private:
    Latency_t clientLat;
};

#endif /* _CLIENT_API_H_ */