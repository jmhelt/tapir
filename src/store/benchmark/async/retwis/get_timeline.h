#ifndef RETWIS_GET_TIMELINE_H
#define RETWIS_GET_TIMELINE_H

#include <functional>
#include <string>
#include <unordered_set>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class GetTimeline : public RetwisTransaction {
   public:
    GetTimeline(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout);
    virtual ~GetTimeline();

    virtual transaction_status_t Execute(SyncClient &client) override;

   private:
    std::unordered_set<std::string> keys_;
};

}  // namespace retwis

#endif /* RETWIS_GET_TIMELINE_H */
