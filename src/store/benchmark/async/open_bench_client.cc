#include "store/benchmark/async/open_bench_client.h"

#include <sys/time.h>

#include <algorithm>
#include <sstream>
#include <string>

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/timeval.h"
#include "lib/transport.h"

DEFINE_LATENCY(op);

OpenBenchmarkClient::OpenBenchmarkClient(AsyncClient &client,
                                         Transport &transport, uint64_t id,
                                         int numRequests, int expDuration,
                                         int warmupSec, int cooldownSec,
                                         uint32_t abortBackoff, bool retryAborted,
                                         uint32_t maxBackoff, uint32_t maxAttempts,
                                         const std::string &latencyFilename)
    : transport_(transport),
      executing_transactions_{},
      next_transaction_id_{0},
      client_{client},
      client_id_(id),
      rand_(id),
      next_arrival_dist_{1e-6},  // TODO: fix this
      numRequests(numRequests),
      expDuration(expDuration),
      warmupSec(warmupSec),
      cooldownSec(cooldownSec),
      latencyFilename(latencyFilename),
      maxBackoff{maxBackoff},
      abortBackoff{abortBackoff},
      retryAborted{retryAborted},
      maxAttempts{maxAttempts} {
    started = false;
    done = false;
    cooldownStarted = false;
    if (numRequests > 0) {
        latencies.reserve(numRequests);
    }
    _Latency_Init(&latency, "txn");
}

OpenBenchmarkClient::~OpenBenchmarkClient() {}

void OpenBenchmarkClient::Start(bench_done_callback bdcb) {
    n = 0;
    curr_bdcb = bdcb;
    transport_.Timer(warmupSec * 1000,
                     std::bind(&OpenBenchmarkClient::WarmupDone, this));
    gettimeofday(&startTime, NULL);

    Latency_Start(&latency);

    transport_.TimerMicro(0, std::bind(&OpenBenchmarkClient::SendNext, this));
}

void OpenBenchmarkClient::SendNext() {
    auto tid = next_transaction_id_;
    Debug("[%lu] SendNext", tid);
    next_transaction_id_++;
    auto transaction = GetNextTransaction();

    executing_transactions_.insert({tid, {tid, transaction}});

    stats.Increment(transaction->GetTransactionType() + "_attempts", 1);

    Latency_Start(&latency);

    auto ecb = std::bind(&OpenBenchmarkClient::ExecuteCallback, this, tid, std::placeholders::_1, std::placeholders::_2);
    client_.Execute(transaction, ecb);

    if (!done) {
        uint64_t next_arrival_us = static_cast<uint64_t>(next_arrival_dist_(rand_));
        Debug("next arrival in %lu us", next_arrival_us);
        transport_.TimerMicro(next_arrival_us, std::bind(&OpenBenchmarkClient::SendNext, this));
    }
}

void OpenBenchmarkClient::ExecuteCallback(uint64_t transaction_id,
                                          transaction_status_t result,
                                          const ReadValueMap &readValues) {
    Debug("[%lu] ExecuteCallback with result %d.", transaction_id, result);
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &ctx = search->second;
    auto transaction = ctx.transaction();
    auto &ttype = transaction->GetTransactionType();
    auto n_attempts = ctx.n_attempts();

    if (result == COMMITTED || result == ABORTED_USER ||
        (maxAttempts != -1 && n_attempts >= static_cast<uint64_t>(maxAttempts)) ||
        !retryAborted) {
        if (result == COMMITTED) {
            stats.Increment(ttype + "_committed", 1);
        }
        if (retryAborted) {
            stats.Add(ttype + "_attempts_list", n_attempts);
        }
        OnReply(transaction_id, result);
    } else {
        stats.Increment(ttype + "_" + std::to_string(result), 1);
        OpenBenchmarkClient::BenchState state = GetBenchState();
        Debug("Current bench state: %d.", state);
        if (state == DONE) {
            OnReply(transaction_id, ABORTED_SYSTEM);
        } else {
            uint64_t backoff = 0;
            if (abortBackoff > 0) {
                uint64_t exp = std::min(n_attempts - 1UL, 56UL);
                Debug("Exp is %lu (min of %lu and 56.", exp, n_attempts - 1UL);
                uint64_t upper = std::min((1UL << exp) * abortBackoff, maxBackoff);
                Debug("Upper is %lu (min of %lu and %lu.", upper, (1UL << exp) * abortBackoff,
                      maxBackoff);
                backoff = std::uniform_int_distribution<uint64_t>(0UL, upper)(GetRand());
                stats.Increment(ttype + "_backoff", backoff);
                Debug("Backing off for %lums", backoff);
            }

            ctx.incr_attempts();
            transport_.TimerMicro(backoff, [this, transaction_id, transaction, ttype]() {
                stats.Increment(ttype + "_attempts", 1);
                auto ecb = std::bind(&OpenBenchmarkClient::ExecuteCallback, this, transaction_id, std::placeholders::_1, std::placeholders::_2);
                client_.Execute(transaction, ecb);
            });
        }
    }
}

void OpenBenchmarkClient::WarmupDone() {
    started = true;
    Notice("Completed warmup period of %d seconds with %d requests", warmupSec, n);
    n = 0;
}

void OpenBenchmarkClient::CooldownDone() {
    done = true;

    char buf[1024];
    Notice("Finished cooldown period.");
    std::sort(latencies.begin(), latencies.end());

    if (latencies.size() > 0) {
        uint64_t ns = latencies[latencies.size() / 2];
        LatencyFmtNS(ns, buf);
        Notice("Median latency is %ld ns (%s)", ns, buf);

        ns = 0;
        for (auto latency : latencies) {
            ns += latency;
        }
        ns = ns / latencies.size();
        LatencyFmtNS(ns, buf);
        Notice("Average latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 90 / 100];
        LatencyFmtNS(ns, buf);
        Notice("90th percentile latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 95 / 100];
        LatencyFmtNS(ns, buf);
        Notice("95th percentile latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 99 / 100];
        LatencyFmtNS(ns, buf);
        Notice("99th percentile latency is %ld ns (%s)", ns, buf);
    }
    curr_bdcb();
}

void OpenBenchmarkClient::OnReply(uint64_t transaction_id, int result) {
    Debug("[%lu] OnReply with result %d.", transaction_id, result);
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &ctx = search->second;
    auto transaction = ctx.transaction();

    if (started) {
        // record latency
        if (!cooldownStarted) {
            uint64_t ns = Latency_End(&latency);
            // TODO: use standard definitions across all clients for
            // success/commit and failure/abort
            if (result == 0) {  // only record result if success
                struct timespec curr;
                clock_gettime(CLOCK_MONOTONIC, &curr);
                if (latencies.size() == 0UL) {
                    gettimeofday(&startMeasureTime, NULL);
                    startMeasureTime.tv_sec -= ns / 1000000000ULL;
                    startMeasureTime.tv_usec -= (ns % 1000000000ULL) / 1000ULL;
                    // std::cout << "#start," << startMeasureTime.tv_sec << ","
                    // << startMeasureTime.tv_usec << std::endl;
                }
                uint64_t currNanos = curr.tv_sec * 1000000000ULL + curr.tv_nsec;
                std::cout << transaction->GetTransactionType() << ',' << ns << ',' << currNanos << ','
                          << client_id_ << std::endl;
                latencies.push_back(ns);
            }
        }

        if (numRequests == -1) {
            struct timeval diff;
            BenchState state = GetBenchState(diff);
            if ((state == COOL_DOWN || state == DONE) && !cooldownStarted) {
                Debug("Starting cooldown after %ld seconds.", diff.tv_sec);
                Finish();
            } else if (state == DONE) {
                Debug("Finished cooldown after %ld seconds.", diff.tv_sec);
                CooldownDone();
            } else {
                Debug("Not done after %ld seconds.", diff.tv_sec);
            }
        } else if (n >= numRequests) {
            CooldownDone();
        }
    }

    delete transaction;
    executing_transactions_.erase(search);

    n++;
}

void OpenBenchmarkClient::StartLatency() { Latency_Start(&latency); }

OpenBenchmarkClient::BenchState OpenBenchmarkClient::GetBenchState(struct timeval &diff) const {
    struct timeval currTime;
    gettimeofday(&currTime, NULL);

    diff = timeval_sub(currTime, startTime);
    if (diff.tv_sec > expDuration) {
        return DONE;
    } else if (diff.tv_sec > expDuration - warmupSec) {
        return COOL_DOWN;
    } else if (started) {
        return MEASURE;
    } else {
        return WARM_UP;
    }
}

OpenBenchmarkClient::BenchState OpenBenchmarkClient::GetBenchState() const {
    struct timeval diff;
    return GetBenchState(diff);
}

void OpenBenchmarkClient::Finish() {
    gettimeofday(&endTime, NULL);
    struct timeval diff = timeval_sub(endTime, startMeasureTime);

    std::cout << "#end," << diff.tv_sec << "," << diff.tv_usec << "," << client_id_
              << std::endl;

    Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds", n,
           VA_TIMEVAL_DIFF(diff));

    if (latencyFilename.size() > 0) {
        Latency_FlushTo(latencyFilename.c_str());
    }

    if (numRequests == -1) {
        cooldownStarted = true;
    } else {
        CooldownDone();
    }
}
