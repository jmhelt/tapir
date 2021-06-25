#ifndef RETWIS_TRANSACTION_H
#define RETWIS_TRANSACTION_H

#include <random>
#include <vector>

#include "store/benchmark/async/common/key_selector.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace retwis {

class RetwisTransaction : public AsyncTransaction {
   public:
    RetwisTransaction(KeySelector *keySelector, int numKeys, std::mt19937 &rand);
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
