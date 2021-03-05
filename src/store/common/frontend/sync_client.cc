#include "store/common/frontend/sync_client.h"

SyncClient::SyncClient(Client *client) : client(client) {}

SyncClient::~SyncClient() {}

void SyncClient::Begin(uint32_t timeout) { client->Begin(); }

void SyncClient::Get(const std::string &key, std::string &value,
                     uint32_t timeout) {
    client->Get(key, value);
}

void SyncClient::Get(const std::string &key, uint32_t timeout) {
    std::string value;
    client->Get(key, value);
}

void SyncClient::Put(const std::string &key, const std::string &value,
                     uint32_t timeout) {
    client->Put(key, value);
}

transaction_status_t SyncClient::Commit(uint32_t timeout) {
    if (client->Commit()) {
        return COMMITTED;
    } else {
        return ABORTED_SYSTEM;
    }
}

void SyncClient::Abort(uint32_t timeout) { client->Abort(); }
