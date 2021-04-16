#include "store/strongstore/preparedtransaction.h"

namespace strongstore {

PreparedTransaction::PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts,
                                         const std::unordered_map<std::string, std::string> &write_set)
    : transaction_id_{transaction_id}, prepare_ts_{prepare_ts}, write_set_{write_set} {}

PreparedTransaction::PreparedTransaction(const proto::PreparedTransactionMessage &msg)
    : transaction_id_{msg.transaction_id()}, prepare_ts_{msg.prepare_timestamp()} {
    for (auto &w : msg.write_set()) {
        write_set_.emplace(w.key(), w.value());
    }
}

PreparedTransaction::~PreparedTransaction() {}

void PreparedTransaction::serialize(proto::PreparedTransactionMessage *msg) const {
    msg->set_transaction_id(transaction_id_);
    prepare_ts_.serialize(msg->mutable_prepare_timestamp());

    for (auto write : write_set_) {
        WriteMessage *writeMsg = msg->add_write_set();
        writeMsg->set_key(write.first);
        writeMsg->set_value(write.second);
    }
}

};  // namespace strongstore