#include "store/benchmark/async/retwis/retwis_client.h"

#include <iostream>

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, Client &client, uint32_t timeout,
                           Transport &transport, uint64_t id, int numRequests, int expDuration,
                           uint64_t delay,
                           int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                           bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, const std::string &latencyFilename)
    : OpenBenchmarkClient(client, timeout, transport, id, numRequests,
                          expDuration,
                          warmupSec, cooldownSec, abortBackoff,
                          retryAborted, maxBackoff, maxAttempts, latencyFilename),
      keySelector(keySelector) {
}

RetwisClient::~RetwisClient() {
}

AsyncTransaction *RetwisClient::GetNextTransaction() {
    int ttype = GetRand()() % 100;
    if (ttype < 5) {
        lastOp = "add_user";
        return new AddUser(keySelector, GetRand());
    } else if (ttype < 20) {
        lastOp = "follow";
        return new Follow(keySelector, GetRand());
    } else if (ttype < 50) {
        lastOp = "post_tweet";
        return new PostTweet(keySelector, GetRand());
    } else {
        lastOp = "get_timeline";
        return new GetTimeline(keySelector, GetRand());
    }
}

}  //namespace retwis
