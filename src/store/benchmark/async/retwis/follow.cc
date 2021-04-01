#include "store/benchmark/async/retwis/follow.h"

namespace retwis {

Follow::Follow(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout)
    : RetwisTransaction(keySelector, 2, rand, timeout) {}

Follow::~Follow() {}

transaction_status_t Follow::Execute(SyncClient &client) {
    Debug("FOLLOW");
    client.Begin(timeout);

    std::string value;
    for (int i = 0; i < 2; i++) {
        if (client.Get(GetKey(i), value, timeout)) {
            client.Abort(timeout);
            return ABORTED_SYSTEM;
        }
        client.Put(GetKey(i), GetKey(i), timeout);
    }

    Debug("COMMIT");
    return client.Commit(timeout);
}

}  // namespace retwis
