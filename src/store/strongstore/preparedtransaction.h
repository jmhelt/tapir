#ifndef _STRONG_PREPARED_TRANSACTION_H_
#define _STRONG_PREPARED_TRANSACTION_H_

#include <cstdint>
#include <string>
#include <unordered_map>

#include "store/common/timestamp.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

class PreparedTransaction {
   public:
    PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts,
                        const std::unordered_map<std::string, std::string> &write_set);
    PreparedTransaction(const proto::PreparedTransactionMessage &msg);
    ~PreparedTransaction();

    void serialize(proto::PreparedTransactionMessage *msg) const;

    uint64_t transaction_id() const { return transaction_id_; }
    const Timestamp &prepare_ts() const { return prepare_ts_; }
    const std::unordered_map<std::string, std::string> &write_set() const { return write_set_; }

   private:
    uint64_t transaction_id_;
    Timestamp prepare_ts_;
    std::unordered_map<std::string, std::string> write_set_;
};

};  // namespace strongstore

#endif /* _STRONG_PREPARED_TRANSACTION_H_ */