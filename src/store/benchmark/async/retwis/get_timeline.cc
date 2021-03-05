#include "store/benchmark/async/retwis/get_timeline.h"

namespace retwis {

GetTimeline::GetTimeline(KeySelector *keySelector, std::mt19937 &rand,
                         uint32_t timeout)
    : RetwisTransaction(keySelector, 1 + rand() % 10, rand, timeout) {}

GetTimeline::~GetTimeline() {}

transaction_status_t GetTimeline::Execute(SyncClient &client) {
    Debug("GET_TIMELINE %lu", GetNumKeys());

    for (std::size_t i = 0; i < GetNumKeys(); i++) {
        client.Get(GetKey(i), timeout);
    }

    Debug("COMMIT");
    return client.Commit(timeout);
}

}  // namespace retwis
