#ifndef OPEN_BENCHMARK_CLIENT_H
#define OPEN_BENCHMARK_CLIENT_H

#include <random>
#include <unordered_map>

#include "lib/latency.h"
#include "lib/transport.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/stats.h"

typedef std::function<void()> bench_done_callback;

class OpenBenchmarkClient {
   public:
    OpenBenchmarkClient(AsyncClient &client,
                        Transport &transport, uint64_t id, int numRequests,
                        int expDuration, int warmupSec, int cooldownSec,
                        uint32_t abortBackoff, bool retryAborted,
                        uint32_t maxBackoff, uint32_t maxAttempts,
                        const std::string &latencyFilename = "");
    virtual ~OpenBenchmarkClient();

    void Start(bench_done_callback bdcb);
    void OnReply(uint64_t transaction_id, int result);

    void StartLatency();

    void SendNext();
    void ExecuteCallback(uint64_t transaction_id, transaction_status_t result, const ReadValueMap &readValues);

    inline bool IsFullyDone() { return done; }

    struct Latency_t latency;
    bool started;
    bool done;
    bool cooldownStarted;
    std::vector<uint64_t> latencies;

    inline const Stats &GetStats() const { return stats; }

   protected:
    virtual AsyncTransaction *GetNextTransaction() = 0;

    inline std::mt19937 &GetRand() { return rand_; }

    enum BenchState { WARM_UP = 0,
                      MEASURE = 1,
                      COOL_DOWN = 2,
                      DONE = 3 };
    BenchState GetBenchState(struct timeval &diff) const;
    BenchState GetBenchState() const;

    Stats stats;
    Transport &transport_;

   private:
    class ExecutingTransactionContext {
       public:
        ExecutingTransactionContext(uint64_t id, AsyncTransaction *transaction)
            : id_{id}, transaction_{transaction}, n_attempts_{1} {}

        const uint64_t id() const { return id_; }
        AsyncTransaction *transaction() const { return transaction_; }

        const uint64_t n_attempts() const { return n_attempts_; }
        void incr_attempts() { n_attempts_++; }

       private:
        uint64_t id_;
        AsyncTransaction *transaction_;
        uint64_t n_attempts_;
    };

    void Finish();
    void WarmupDone();
    void CooldownDone();

    std::unordered_map<uint64_t, ExecutingTransactionContext> executing_transactions_;
    uint64_t next_transaction_id_;

    AsyncClient &client_;

    const uint64_t client_id_;
    int tputInterval;
    std::mt19937 rand_;
    std::exponential_distribution<> next_arrival_dist_;
    int numRequests;
    int expDuration;
    int n;
    int warmupSec;
    int cooldownSec;
    struct timeval startTime;
    struct timeval endTime;
    struct timeval startMeasureTime;
    string latencyFilename;
    int msSinceStart;
    int opLastInterval;
    bench_done_callback curr_bdcb;
    std::uniform_int_distribution<uint64_t> randDelayDist;

    uint64_t maxBackoff;
    uint64_t abortBackoff;
    bool retryAborted;
    int64_t maxAttempts;
};

#endif /* OPEN_BENCHMARK_CLIENT_H */
