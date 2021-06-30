#include "store/benchmark/async/open_bench_client.h"

#include <sys/time.h>

#include <algorithm>
#include <sstream>
#include <string>

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/timeval.h"
#include "lib/transport.h"
#include "store/strongstore/client.h"

DEFINE_LATENCY(op);

OpenBenchmarkClient::OpenBenchmarkClient(Client &client, uint32_t timeout,
                                         Transport &transport, uint64_t id,
                                         double arrival_rate,
                                         int numRequests, int expDuration,
                                         int warmupSec, int cooldownSec,
                                         uint32_t abortBackoff, bool retryAborted,
                                         uint32_t maxBackoff, uint32_t maxAttempts,
                                         const std::string &latencyFilename)
    : transport_(transport),
      executing_transactions_{},
      next_transaction_id_{1},
      client_{client},
      client_id_{id},
      timeout_{timeout},
      rand_{id},
      next_arrival_dist_{arrival_rate * 1e-6},
      n_requests_(numRequests),
      exp_duration_(expDuration),
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

    if (arrival_rate <= 0) {
        Panic("Arrival rate must be (strictly) positive!");
    }

    _Latency_Init(&latency, "txn");
}

OpenBenchmarkClient::~OpenBenchmarkClient() {
    Debug("executing_transactions_.size(): %lu", executing_transactions_.size());
}

void OpenBenchmarkClient::Start(bench_done_callback bdcb) {
    n = 0;
    curr_bdcb_ = bdcb;
    transport_.Timer(warmupSec * 1000, std::bind(&OpenBenchmarkClient::WarmupDone, this));
    gettimeofday(&startTime, NULL);

    transport_.TimerMicro(0, std::bind(&OpenBenchmarkClient::SendNext, this));
}

void OpenBenchmarkClient::SendNext() {
    auto tid = next_transaction_id_;
    Debug("[%lu] SendNext", tid);
    next_transaction_id_++;

    auto transaction = GetNextTransaction();
    stats.Increment(transaction->GetTransactionType() + "_attempts", 1);

    auto bcb = std::bind(&OpenBenchmarkClient::BeginCallback, this, tid, transaction, std::placeholders::_1);
    auto btcb = []() {};
    client_.Begin(bcb, btcb, timeout_);

    if (!done) {
        uint64_t next_arrival_us = static_cast<uint64_t>(next_arrival_dist_(rand_));
        Debug("next arrival in %lu us", next_arrival_us);
        transport_.TimerMicro(next_arrival_us, std::bind(&OpenBenchmarkClient::SendNext, this));
    }
}

void OpenBenchmarkClient::BeginCallback(const uint64_t transaction_id, AsyncTransaction *transaction, Context &ctx) {
    auto ecb = std::bind(&OpenBenchmarkClient::ExecuteCallback, this, transaction_id, std::placeholders::_1);

    executing_transactions_.insert({transaction_id, {transaction_id, transaction, ctx, ecb}});

    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());
    _Latency_StartRec(search->second.lat());

    ExecuteNextOperation(transaction_id);
}

void OpenBenchmarkClient::ExecuteNextOperation(const uint64_t transaction_id) {
    Debug("[%lu] ExecuteNextOperation", transaction_id);
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;
    auto transaction = et.transaction();
    auto op_index = et.op_index();
    auto &ctx = et.ctx();

    Operation op = transaction->GetNextOperation(op_index);
    et.incr_op_index();

    auto gcb = std::bind(&OpenBenchmarkClient::GetCallback, this, transaction_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    auto gtcb = std::bind(&OpenBenchmarkClient::GetTimeout, this, transaction_id, std::placeholders::_1, std::placeholders::_2);
    auto pcb = std::bind(&OpenBenchmarkClient::PutCallback, this, transaction_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto ptcb = std::bind(&OpenBenchmarkClient::PutTimeout, this, transaction_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto ccb = std::bind(&OpenBenchmarkClient::CommitCallback, this, transaction_id, std::placeholders::_1);
    auto ctcb = std::bind(&OpenBenchmarkClient::CommitTimeout, this);
    auto acb = std::bind(&OpenBenchmarkClient::AbortCallback, this);
    auto atcb = std::bind(&OpenBenchmarkClient::AbortTimeout, this);

    switch (op.type) {
        case GET:
            client_.Get(ctx, op.key, gcb, gtcb, timeout_);
            break;

        case GET_FOR_UPDATE:
            client_.GetForUpdate(ctx, op.key, gcb, gtcb, timeout_);
            break;

        case PUT:
            client_.Put(ctx, op.key, op.value, pcb, ptcb, timeout_);
            break;

        case COMMIT:
            client_.Commit(ctx, ccb, ctcb, timeout_);
            break;

        case ABORT:
            client_.Abort(ctx, acb, atcb, timeout_);
            break;

        case ROCOMMIT:
            client_.ROCommit(ctx, op.keys, ccb, ctcb, timeout_);
            break;

        case WAIT:
            break;

        default:
            NOT_REACHABLE();
    }
}

void OpenBenchmarkClient::GetCallback(const uint64_t transaction_id,
                                      int status, const std::string &key, const std::string &val, Timestamp ts) {
    Debug("[%lu] Get(%s) callback", transaction_id, key.c_str());
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;

    if (status == REPLY_OK) {
        ExecuteNextOperation(transaction_id);
    } else if (status == REPLY_FAIL) {
        auto ecb = et.ecb();
        ecb(ABORTED_SYSTEM);
    } else {
        Panic("Unknown status for Get %d.", status);
    }
}

void OpenBenchmarkClient::GetTimeout(const uint64_t transaction_id,
                                     int status, const std::string &key) {
    Warning("[%lu] Get(%s) timed out :(", transaction_id, key.c_str());
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;

    auto &ctx = et.ctx();
    auto gcb = std::bind(&OpenBenchmarkClient::GetCallback, this, transaction_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    auto gtcb = std::bind(&OpenBenchmarkClient::GetTimeout, this, transaction_id, std::placeholders::_1, std::placeholders::_2);

    client_.Get(ctx, key, gcb, gtcb, timeout_);
}

void OpenBenchmarkClient::PutCallback(const uint64_t transaction_id,
                                      int status, const std::string &key, const std::string &val) {
    Debug("[%lu] Put(%s,%s) callback.", transaction_id, key.c_str(), val.c_str());
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;

    if (status == REPLY_OK) {
        ExecuteNextOperation(transaction_id);
    } else if (status == REPLY_FAIL) {
        auto ecb = et.ecb();
        ecb(ABORTED_SYSTEM);
    } else {
        Panic("Unknown status for Put %d.", status);
    }
}

void OpenBenchmarkClient::PutTimeout(const uint64_t transaction_id,
                                     int status, const std::string &key, const std::string &val) {
    Warning("[%lu] Put(%s,%s) timed out :(", transaction_id, key.c_str(), val.c_str());
}

void OpenBenchmarkClient::CommitCallback(const uint64_t transaction_id, transaction_status_t result) {
    Debug("Commit callback.");
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;
    auto ecb = et.ecb();

    ecb(result);
}

void OpenBenchmarkClient::CommitTimeout() {
    Warning("Commit timed out :(");
}

void OpenBenchmarkClient::AbortCallback() {
    Debug("Abort callback.");
}

void OpenBenchmarkClient::AbortTimeout() {
    Warning("Abort timed out :(");
}

void OpenBenchmarkClient::ExecuteCallback(uint64_t transaction_id,
                                          transaction_status_t result) {
    Debug("[%lu] ExecuteCallback with result %d.", transaction_id, result);
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;
    auto transaction = et.transaction();
    auto &ttype = transaction->GetTransactionType();
    auto n_attempts = et.n_attempts();

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

            et.incr_attempts();

            transport_.TimerMicro(backoff, [this, transaction_id]() {
                auto search = executing_transactions_.find(transaction_id);
                ASSERT(search != executing_transactions_.end());

                auto &et = search->second;
                auto transaction = et.transaction();
                auto ctx = et.ctx();
                auto &ttype = et.transaction()->GetTransactionType();
                executing_transactions_.erase(search);

                stats.Increment(ttype + "_attempts", 1);

                auto bcb = std::bind(&OpenBenchmarkClient::BeginCallback, this, transaction_id, transaction, std::placeholders::_1);
                auto btcb = []() {};
                client_.Begin(ctx, bcb, btcb, timeout_);
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
    curr_bdcb_();
}

void OpenBenchmarkClient::OnReply(uint64_t transaction_id, int result) {
    Debug("[%lu] OnReply with result %d.", transaction_id, result);
    auto search = executing_transactions_.find(transaction_id);
    ASSERT(search != executing_transactions_.end());

    auto &et = search->second;
    auto transaction = et.transaction();
    auto lat = et.lat();

    if (started) {
        // record latency
        if (!cooldownStarted) {
            _Latency_EndRec(&latency, lat);
            uint64_t ns = lat->accum;
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

        if (n_requests_ == -1) {
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
        } else if (n >= n_requests_) {
            CooldownDone();
        }
    }

    delete transaction;
    executing_transactions_.erase(search);

    n++;
}

OpenBenchmarkClient::BenchState OpenBenchmarkClient::GetBenchState(struct timeval &diff) const {
    struct timeval currTime;
    gettimeofday(&currTime, NULL);

    diff = timeval_sub(currTime, startTime);
    if (diff.tv_sec > exp_duration_) {
        return DONE;
    } else if (diff.tv_sec > exp_duration_ - warmupSec) {
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

    if (n_requests_ == -1) {
        cooldownStarted = true;
    } else {
        CooldownDone();
    }
}
