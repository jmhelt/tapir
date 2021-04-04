#include "store/benchmark/async/retwis/get_timeline.h"

namespace retwis {

GetTimeline::GetTimeline(KeySelector *keySelector, std::mt19937 &rand,
                         uint32_t timeout)
    : RetwisTransaction(keySelector, 1 + rand() % 10, rand, timeout), keys_{} {}

GetTimeline::~GetTimeline() {}

transaction_status_t GetTimeline::Execute(SyncClient &client, bool is_retry) {
    (void)is_retry;
    Debug("GET_TIMELINE %lu", GetNumKeys());

    keys_.clear();
    for (std::size_t i = 0; i < GetNumKeys(); i++) {
        keys_.insert(GetKey(i));
    }

    Debug("RO COMMIT");
    return client.ROCommit(keys_, timeout);
}

}  // namespace retwis
