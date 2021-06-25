#ifndef ASYNC_TRANSACTION_BENCH_CLIENT_H
#define ASYNC_TRANSACTION_BENCH_CLIENT_H

#include <random>

#include "store/benchmark/async/bench_client.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

class AsyncTransactionBenchClient : public BenchmarkClient {
   public:
    AsyncTransactionBenchClient(AsyncClient *client,
                                Transport &transport, uint64_t id, int numRequests, int expDuration,
                                uint64_t delay,
                                int warmupSec, int cooldownSec, int tputInterval, uint64_t abortBackoff,
                                bool retryAborted, uint64_t maxBackoff, int64_t maxAttempts,
                                const std::string &latencyFilename = "");

    virtual ~AsyncTransactionBenchClient();

   protected:
    virtual AsyncTransaction *GetNextTransaction() = 0;
    virtual ::context::AsyncTransaction *GetNextContextTransaction() { return nullptr; }
    virtual void SendNext();

    void ExecuteCallback(transaction_status_t result, const ReadValueMap &readValues);

    AsyncClient *client;

   private:
    uint64_t maxBackoff;
    uint64_t abortBackoff;
    bool retryAborted;
    int64_t maxAttempts;
    AsyncTransaction *currTxn;
    ::context::AsyncTransaction *context_txn;
    uint64_t currTxnAttempts;
};

#endif /* ASYNC_TRANSACTION_BENCH_CLIENT_H */
