#ifndef RETWIS_CLIENT_H
#define RETWIS_CLIENT_H

#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/open_bench_client.h"
#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

enum KeySelection {
    UNIFORM,
    ZIPF
};

class RetwisClient : public OpenBenchmarkClient {
   public:
    RetwisClient(KeySelector *keySelector, AsyncClient &client,
                 Transport &transport, uint64_t id, int numRequests, int expDuration,
                 uint64_t delay, int warmupSec, int cooldownSec, int tputInterval,
                 uint32_t abortBackoff,
                 bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                 const std::string &latencyFilename = "");

    virtual ~RetwisClient();

   protected:
    virtual AsyncTransaction *GetNextTransaction() override;

   private:
    KeySelector *keySelector;
    std::string lastOp;
};

}  //namespace retwis

#endif /* RETWIS_CLIENT_H */
