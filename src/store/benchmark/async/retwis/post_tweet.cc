#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

PostTweet::PostTweet(KeySelector *keySelector, std::mt19937 &rand,
                     uint32_t timeout)
    : RetwisTransaction(keySelector, 5, rand, timeout) {}

PostTweet::~PostTweet() {}

transaction_status_t PostTweet::Execute(SyncClient &client) {
    Debug("POST_TWEET");
    client.Begin(timeout);

    for (int i = 0; i < 3; i++) {
        client.Get(GetKey(i), timeout);
        client.Put(GetKey(i), GetKey(i), timeout);
    }

    client.Put(GetKey(3), GetKey(3), timeout);
    client.Put(GetKey(4), GetKey(4), timeout);

    Debug("COMMIT");
    return client.Commit(timeout);
}

}  // namespace retwis
