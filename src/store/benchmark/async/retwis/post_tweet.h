#ifndef RETWIS_POST_TWEET_H
#define RETWIS_POST_TWEET_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class PostTweet : public RetwisTransaction {
   public:
    PostTweet(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout);
    virtual ~PostTweet();

    virtual transaction_status_t Execute(SyncClient &client,
                                         bool is_retry) override;
};

}  // namespace retwis

#endif /* RETWIS_POST_TWEET_H */
