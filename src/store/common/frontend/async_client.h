#ifndef ASYNC_CLIENT_H
#define ASYNC_CLIENT_H

#include "lib/latency.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/stats.h"

#include <functional>

#define SUCCESS 0
#define FAILED 1
typedef std::function<void(transaction_status_t, const ReadValueMap &)> execute_callback;

class AsyncClient {
 public:
  AsyncClient() {
    _Latency_Init(&clientLat, "client_lat");
  }
  
  virtual ~AsyncClient() {}

  // Begin a transaction.
  virtual void Execute(AsyncTransaction *txn, execute_callback ecb) = 0;

  inline Stats &GetStats() { return stats; }
 protected:
  void StartRecLatency();
  void EndRecLatency(const std::string &str);
  Stats stats;
 private:
  Latency_t clientLat;
};

#endif /* ASYNC_CLIENT_H */
