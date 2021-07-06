#ifndef OPEN_BENCHMARK_CLIENT_H
#define OPEN_BENCHMARK_CLIENT_H

#include <functional>
#include <random>
#include <unordered_map>

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"

typedef std::function<void(transaction_status_t)> execute_callback;

typedef std::function<void()> bench_done_callback;

class OpenBenchmarkClient {
   public:
    OpenBenchmarkClient(Client &client, uint32_t timeout,
                        Transport &transport, uint64_t id,
                        double arrival_rate, double think_time, double stay_probability,
                        int expDuration, int warmupSec, int cooldownSec,
                        uint32_t abortBackoff, bool retryAborted,
                        uint32_t maxBackoff, uint32_t maxAttempts,
                        const std::string &latencyFilename = "");
    virtual ~OpenBenchmarkClient();

    void Start(bench_done_callback bdcb);
    void OnReply(uint64_t transaction_id, int result);

    void SendNext();
    void ExecuteCallback(uint64_t transaction_id, transaction_status_t result);

    inline bool IsFullyDone() { return done; }

    struct Latency_t latency;
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
    class ExecutingTransaction {
       public:
        ExecutingTransaction(uint64_t id, AsyncTransaction *transaction, Context &ctx, execute_callback ecb)
            : lat_{}, id_{id}, transaction_{transaction}, ctx_{ctx}, ecb_{ecb}, n_attempts_{1}, op_index_{0} {}

        uint64_t id() const { return id_; }
        AsyncTransaction *transaction() const { return transaction_; }
        Context &ctx() { return ctx_; }
        execute_callback ecb() const { return ecb_; }

        Latency_Frame_t *lat() { return &lat_; }

        uint64_t n_attempts() const { return n_attempts_; }
        void incr_attempts() { n_attempts_++; }

        uint64_t op_index() const { return op_index_; }
        void reset_outstanding_ops() { op_index_ = 0; }
        void incr_op_index() { op_index_++; }

       private:
        Latency_Frame_t lat_;
        uint64_t id_;
        AsyncTransaction *transaction_;
        Context ctx_;
        execute_callback ecb_;
        uint64_t n_attempts_;
        std::size_t op_index_;
    };

    void SendNextInSession(Context ctx);

    void BeginCallback(const uint64_t transaction_id, AsyncTransaction *transaction, Context &ctx);

    void ExecuteNextOperation(const uint64_t transaction_id);

    void GetCallback(const uint64_t transaction_id,
                     int status, const std::string &key, const std::string &val, Timestamp ts);
    void GetTimeout(const uint64_t transaction_id,
                    int status, const std::string &key);

    void PutCallback(const uint64_t transaction_id,
                     int status, const std::string &key, const std::string &val);
    void PutTimeout(const uint64_t transaction_id,
                    int status, const std::string &key, const std::string &val);

    void CommitCallback(const uint64_t transaction_id, transaction_status_t result);
    void CommitTimeout();
    void AbortCallback();
    void AbortTimeout();

    void Finish();
    void WarmupDone();
    void CooldownDone();

    std::unordered_map<uint64_t, ExecutingTransaction> executing_transactions_;
    uint64_t next_transaction_id_;

    Client &client_;

    const uint64_t client_id_;
    uint32_t timeout_;
    std::mt19937 rand_;
    std::exponential_distribution<> next_arrival_dist_;
    std::exponential_distribution<> think_time_dist_;
    std::bernoulli_distribution stay_dist_;
    int exp_duration_;
    int n;
    int warmupSec;
    int cooldownSec;
    struct timeval startTime;
    struct timeval endTime;
    struct timeval startMeasureTime;
    string latencyFilename;
    int msSinceStart;
    bench_done_callback curr_bdcb_;

    uint64_t maxBackoff;
    uint64_t abortBackoff;
    bool retryAborted;
    int64_t maxAttempts;

    bool started;
    bool done;
    bool cooldownStarted;
};

#endif /* OPEN_BENCHMARK_CLIENT_H */
