#ifndef RETWIS_CLIENT_H
#define RETWIS_CLIENT_H

#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/retwis/retwis_transaction.h"
#include "store/benchmark/async/sync_transaction_bench_client.h"

namespace retwis {

enum KeySelection { UNIFORM, ZIPF };

class RetwisClient : public SyncTransactionBenchClient {
   public:
    RetwisClient(KeySelector *keySelector, SyncClient &client,
                 Transport &transport, uint64_t seed, int numRequests,
                 int expDuration, uint64_t delay, int warmupSec,
                 int cooldownSec, int tputInterval, uint32_t abortBackoff,
                 bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                 uint32_t timeout, const std::string &latencyFilename = "");

    virtual ~RetwisClient();

   protected:
    virtual SyncTransaction *GetNextTransaction();
    virtual std::string GetLastOp() const;

   private:
    KeySelector *keySelector;
    std::string lastOp;
};

}  // namespace retwis

#endif /* RETWIS_CLIENT_H */
