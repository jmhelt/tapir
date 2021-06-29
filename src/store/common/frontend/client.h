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

class Context {
   public:
    Context() : transaction_id_{0}, start_ts_{0, 0} {}
    Context(uint64_t transaction_id, const Timestamp &start_ts)
        : transaction_id_{transaction_id}, start_ts_{start_ts} {}

    const uint64_t transaction_id() const { return transaction_id_; }
    const Timestamp &start_ts() const { return start_ts_; }

   private:
    const uint64_t transaction_id_;
    const Timestamp start_ts_;
};

typedef std::function<void(Context &)> begin_callback;
typedef std::function<void()> begin_timeout_callback;

typedef std::function<void(int, const std::string &, const std::string &, Timestamp)> get_callback;
typedef std::function<void(int, const std::string &)> get_timeout_callback;

typedef std::function<void(int, const std::string &, const std::string &)>
    put_callback;
typedef std::function<void(int, const std::string &, const std::string &)>
    put_timeout_callback;

typedef std::function<void(transaction_status_t)> commit_callback;
typedef std::function<void()> commit_timeout_callback;

typedef std::function<void()> abort_callback;
typedef std::function<void()> abort_timeout_callback;

class Client {
   public:
    Client() { _Latency_Init(&clientLat, "client_lat"); }
    virtual ~Client() {}

    virtual void Begin(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) = 0;

    virtual void Begin(const Context &ctx, begin_callback bcb,
                       begin_timeout_callback btcb, uint32_t timeout) = 0;

    // Get the value corresponding to key.
    virtual void Get(const Context &ctx, const std::string &key, get_callback gcb,
                     get_timeout_callback gtcb, uint32_t timeout) = 0;

    // Get the value corresponding to key.
    // Provide hint that transaction will later write the key.
    virtual void GetForUpdate(const Context &ctx, const std::string &key, get_callback gcb,
                              get_timeout_callback gtcb, uint32_t timeout) {
        Get(ctx, key, gcb, gtcb, timeout);
    }

    // Set the value for the given key.
    virtual void Put(const Context &ctx, const std::string &key, const std::string &value,
                     put_callback pcb, put_timeout_callback ptcb,
                     uint32_t timeout) = 0;

    // Commit all Get(s) and Put(s) since Begin().
    virtual void Commit(const Context &ctx, commit_callback ccb, commit_timeout_callback ctcb,
                        uint32_t timeout) = 0;

    // Abort all Get(s) and Put(s) since Begin().
    virtual void Abort(const Context &ctx, abort_callback acb, abort_timeout_callback atcb,
                       uint32_t timeout) = 0;

    virtual void ROCommit(const Context &ctx, const std::unordered_set<std::string> &keys,
                          commit_callback ccb, commit_timeout_callback ctcb,
                          uint32_t timeout) {
        Panic("Unimplemented ROCommit!");
    }

    inline Stats &GetStats() { return stats; }

   protected:
    void StartRecLatency();
    void EndRecLatency(const std::string &str);
    Stats stats;

   private:
    Latency_t clientLat;
};

#endif /* _CLIENT_API_H_ */