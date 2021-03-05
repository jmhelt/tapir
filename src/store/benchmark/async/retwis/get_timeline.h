#ifndef RETWIS_GET_TIMELINE_H
#define RETWIS_GET_TIMELINE_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class GetTimeline : public RetwisTransaction {
   public:
    GetTimeline(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout);
    virtual ~GetTimeline();

    virtual transaction_status_t Execute(SyncClient &client) override;
};

}  // namespace retwis

#endif /* RETWIS_GET_TIMELINE_H */
