#include "store/benchmark/async/retwis/add_user.h"

namespace retwis {

AddUser::AddUser(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout)
    : RetwisTransaction(keySelector, 3, rand, timeout) {}

AddUser::~AddUser() {}

transaction_status_t AddUser::Execute(SyncClient &client) {
    Debug("ADD_USER");
    client.Begin(timeout);

    std::string value;
    if (client.Get(GetKey(0), value, timeout)) {
        client.Abort(timeout);
        // TODO: Send abort
        return ABORTED_SYSTEM;
    }

    for (int i = 0; i < 3; i++) {
        client.Put(GetKey(i), GetKey(i), timeout);
    }

    Debug("COMMIT");
    return client.Commit(timeout);
}

}  // namespace retwis
