#include "store/benchmark/async/retwis/retwis_client.h"

#include <iostream>

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, SyncClient &client,
                           Transport &transport, uint64_t seed, int numRequests,
                           int expDuration, uint64_t delay, int warmupSec,
                           int cooldownSec, int tputInterval,
                           uint32_t abortBackoff, bool retryAborted,
                           uint32_t maxBackoff, uint32_t maxAttempts,
                           uint32_t timeout, const std::string &latencyFilename)
    : SyncTransactionBenchClient(
          client, transport, seed, numRequests, expDuration, delay, warmupSec,
          cooldownSec, tputInterval, abortBackoff, retryAborted, maxBackoff,
          maxAttempts, timeout, latencyFilename),
      keySelector(keySelector) {}

RetwisClient::~RetwisClient() {}

SyncTransaction *RetwisClient::GetNextTransaction() {
    int ttype = 0;  // GetRand()() % 100;
    if (ttype < 5) {
        lastOp = "add_user";
        return new AddUser(keySelector, GetRand(), GetTimeout());
    } else if (ttype < 20) {
        lastOp = "follow";
        return new Follow(keySelector, GetRand(), GetTimeout());
    } else if (ttype < 50) {
        lastOp = "post_tweet";
        return new PostTweet(keySelector, GetRand(), GetTimeout());
    } else {
        lastOp = "get_timeline";
        return new GetTimeline(keySelector, GetRand(), GetTimeout());
    }
}

std::string RetwisClient::GetLastOp() const { return lastOp; }

}  // namespace retwis
