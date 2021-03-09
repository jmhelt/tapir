#include "store/benchmark/async/retwis/add_user.h"

namespace retwis {

AddUser::AddUser(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout)
    : RetwisTransaction(keySelector, 4, rand, timeout) {}

AddUser::~AddUser() {}

transaction_status_t AddUser::Execute(SyncClient &client) {
    Debug("ADD_USER");
    client.Begin(timeout);

    // client.Get(GetKey(0), timeout);
    client.Put(GetKey(0), GetKey(0), timeout);

    for (int i = 0; i < 3; i++) {
        client.Put(GetKey(i), GetKey(i), timeout);
    }

    Debug("COMMIT");
    return client.Commit(timeout);
}

}  // namespace retwis
