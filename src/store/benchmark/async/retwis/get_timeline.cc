#include "store/benchmark/async/retwis/get_timeline.h"

#include <unordered_set>

namespace retwis {

GetTimeline::GetTimeline(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 1 + rand() % 10, rand) {
}

GetTimeline::~GetTimeline() {
}

Operation GetTimeline::GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
                                        const ReadValueMap &readValues) {
    Debug("GET_TIMELINE %lu %lu %lu", GetNumKeys(), outstandingOpCount,
          finishedOpCount);

    if (outstandingOpCount == 0) {
        std::unordered_set<std::string> keys;
        for (std::size_t i = 0; i < GetNumKeys(); i++) {
            keys.insert(GetKey(i));
        }

        return ROCommit(std::move(keys));
    } else {
        return Wait();
    }
}

}  // namespace retwis
