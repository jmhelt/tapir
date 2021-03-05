#include "store/benchmark/async/retwis/get_timeline.h"

namespace retwis {

GetTimeline::GetTimeline(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 1 + rand() % 10, rand) {}

GetTimeline::~GetTimeline() {}

transaction_status_t GetTimeline::Execute(SyncClient &client) {
    Debug("GET_TIMELINE %lu", GetNumKeys());

    for (int i = 0; i < GetNumKeys(); i++) {
        client.Get(GetKey(i), timeout);
    }

    Debug("COMMIT");
    client.Commit(timeout);
}

}  // namespace retwis
