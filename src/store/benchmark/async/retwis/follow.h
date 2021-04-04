#ifndef RETWIS_FOLLOW_H
#define RETWIS_FOLLOW_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class Follow : public RetwisTransaction {
   public:
    Follow(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout);
    virtual ~Follow();

    virtual transaction_status_t Execute(SyncClient &client,
                                         bool is_retry) override;
};

}  // namespace retwis

#endif /* RETWIS_FOLLOW_H */
