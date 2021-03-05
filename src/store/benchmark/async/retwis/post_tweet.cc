#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

PostTweet::PostTweet(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 5, rand) {}

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
    client.Commit(timeout);
}

}  // namespace retwis
