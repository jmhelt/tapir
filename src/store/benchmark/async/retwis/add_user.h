#ifndef RETWIS_ADD_USER_H
#define RETWIS_ADD_USER_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"
#include "store/common/frontend/sync_client.h"

namespace retwis {

class AddUser : public RetwisTransaction {
   public:
    AddUser(KeySelector *keySelector, std::mt19937 &rand, uint32_t timeout);
    virtual ~AddUser();

    virtual transaction_status_t Execute(SyncClient &client) override;
};

}  // namespace retwis

#endif /* RETWIS_ADD_USER_H */
