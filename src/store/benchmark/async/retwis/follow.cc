#include "store/benchmark/async/retwis/follow.h"

namespace retwis {

Follow::Follow(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 2, rand) {}

Follow::~Follow() {}

transaction_status_t Follow::Execute(SyncClient &client) {
    Debug("FOLLOW");
    client.Begin(timeout);

    for (int i = 0; i < 2; i++) {
        client.Get(GetKey(0), timeout);
        client.Put(GetKey(0), GetKey(0), timeout);
    }

    Debug("COMMIT");
    client.Commit(timeout);
}

}  // namespace retwis
