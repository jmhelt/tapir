#ifndef RETWIS_FOLLOW_H
#define RETWIS_FOLLOW_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class Follow : public RetwisTransaction {
   public:
    Follow(KeySelector *keySelector, std::mt19937 &rand);
    virtual ~Follow();

    virtual transaction_status_t Execute(SyncClient &client) override;
};

}  // namespace retwis

#endif /* RETWIS_FOLLOW_H */
