#ifndef RETWIS_TRANSACTION_H
#define RETWIS_TRANSACTION_H

#include <random>
#include <vector>

#include "store/benchmark/async/common/key_selector.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/sync_transaction.h"

namespace retwis {

class RetwisTransaction : public SyncTransaction {
   public:
    RetwisTransaction(KeySelector *keySelector, int numKeys, std::mt19937 &rand,
                      uint32_t timeout);
    virtual ~RetwisTransaction();

   protected:
    inline const std::string &GetKey(int i) const {
        return keySelector->GetKey(keyIdxs[i]);
    }

    inline const size_t GetNumKeys() const { return keyIdxs.size(); }

    KeySelector *keySelector;

   private:
    std::vector<int> keyIdxs;
};

}  // namespace retwis

#endif /* RETWIS_TRANSACTION_H */
