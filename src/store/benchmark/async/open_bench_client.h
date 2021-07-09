#ifndef OPEN_BENCHMARK_CLIENT_H
#define OPEN_BENCHMARK_CLIENT_H

#include <functional>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

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
    OpenBenchmarkClient(const std::vector<Client *> &clients, uint32_t timeout,
                        Transport &transport, uint64_t id,
                        double arrival_rate, double think_time, double stay_probability,
                        int expDuration, int warmupSec, int cooldownSec,
                        uint32_t abortBackoff, bool retryAborted,
                        uint32_t maxBackoff, uint32_t maxAttempts,
                        const std::string &latencyFilename = "");
    virtual ~OpenBenchmarkClient();

    void Start(bench_done_callback bdcb);
    void OnReply(uint64_t transaction_id, int result, bool erase_et = true);

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
        ExecutingTransaction(uint64_t id, AsyncTransaction *transaction, std::unique_ptr<Context> ctx, execute_callback ecb, std::size_t client_index)
            : lat_{}, id_{id}, transaction_{transaction}, ctx_{std::move(ctx)}, ecb_{ecb}, n_attempts_{1}, op_index_{1}, current_client_index_{client_index}, current_client_txn_count_{0} {}

        uint64_t id() const { return id_; }
        AsyncTransaction *transaction() const { return transaction_; }
        std::unique_ptr<Context> &ctx() { return ctx_; }
        execute_callback ecb() const { return ecb_; }

        Latency_Frame_t *lat() { return &lat_; }

        uint64_t n_attempts() const { return n_attempts_; }
        void incr_attempts() { n_attempts_++; }

        uint64_t op_index() const { return op_index_; }
        void reset_outstanding_ops() { op_index_ = 0; }
        void incr_op_index() { op_index_++; }

        std::size_t current_client_index() const { return current_client_index_; }
        void set_client_index(std::size_t i) { current_client_index_ = i; }

        std::size_t current_client_txn_count() const { return current_client_txn_count_; }
        void incr_current_client_op_count() { current_client_txn_count_++; }

       private:
        Latency_Frame_t lat_;
        uint64_t id_;
        AsyncTransaction *transaction_;
        std::unique_ptr<Context> ctx_;
        execute_callback ecb_;
        uint64_t n_attempts_;
        std::size_t op_index_;
        std::size_t current_client_index_;
        std::size_t current_client_txn_count_;
    };

    void ExecuteAbort(const uint64_t transaction_id, transaction_status_t status);

    void SendNextInSession(std::unique_ptr<Context> &ctx);

    void BeginCallback(uint64_t transaction_id, AsyncTransaction *transaction,
                       std::size_t client_index, std::unique_ptr<Context> ctx);

    void ExecuteNextOperation(const uint64_t transaction_id);

    void GetCallback(const uint64_t transaction_id,
                     int status, const std::string &key, const std::string &val, Timestamp ts);
    void GetTimeout(const uint64_t transaction_id,
                    int status, const std::string &key);

    void PutCallback(const uint64_t transaction_id,
                     int status, const std::string &key, const std::string &val);
    void PutTimeout(const uint64_t transaction_id,
                    int status, const std::string &key, const std::string &val);

    void CommitCallback(const uint64_t transaction_id, transaction_status_t status);
    void CommitTimeout();
    void AbortCallback(const uint64_t transaction_id, transaction_status_t status);
    void AbortTimeout();

    void Finish();
    void WarmupDone();
    void CooldownDone();

    std::unordered_map<uint64_t, ExecutingTransaction> executing_transactions_;
    uint64_t next_transaction_id_;

    const std::vector<Client *> &clients_;

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
