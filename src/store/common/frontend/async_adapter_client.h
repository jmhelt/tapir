#ifndef ASYNC_ADAPTER_CLIENT_H
#define ASYNC_ADAPTER_CLIENT_H

#include "store/common/frontend/async_client.h"

class AsyncAdapterClient : public AsyncClient {
 public:
  AsyncAdapterClient(Client *client, uint32_t timeout);
  virtual ~AsyncAdapterClient();

  // Begin a transaction.
  virtual void Execute(AsyncTransaction *txn, execute_callback ecb);

 private:
  void ExecuteNextOperation();
  void GetCallback(const std::string *keyPtr, int status, const std::string &key,
      const std::string &val,
      Timestamp ts);
  void GetTimeout(const std::string *keyPtr, int status, const std::string &key);
  void PutCallback(int status, const std::string &key, const std::string &val);
  void PutTimeout(int status, const std::string &key, const std::string &val);
  void CommitCallback(transaction_status_t result);
  void CommitTimeout();
  void AbortCallback();
  void AbortTimeout();

  Client *client;
  uint32_t timeout;
  size_t outstandingOpCount;
  size_t finishedOpCount;
  ReadValueMap readValues;
  std::set<std::string> strs;
  execute_callback currEcb;
  AsyncTransaction *currTxn;

};

#endif /* ASYNC_ADAPTER_CLIENT_H */
