// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-

#ifndef _STRONG_INTERSHARDCLIENT_H_
#define _STRONG_INTERSHARDCLIENT_H_

#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"

#include <vector>


namespace strongstore
{

    class InterShardClient
    {
    public:
        InterShardClient(transport::Configuration &config, Transport *transport, int nShards);
        ~InterShardClient();

        void PrepareOK(int coordShard, uint64_t txnID, int participantShard, uint64_t prepareTS);
        void PrepareAbort(int coordShard, uint64_t txnID);

        void PrepareOKCallback(int coordShard, uint64_t txnID, int participantShard, const string &request_str, const string &reply_str);

    private:
        uint64_t clientID;
        int nShards;
        std::vector<ShardClient*> sclient;
    };

} // namespace strongstore

#endif /* _STRONG_INTERSHARDCLIENT_H_ */
