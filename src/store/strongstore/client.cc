// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/client.cc:
 *   Client to transactional storage system with strong consistency
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "store/strongstore/client.h"

using namespace std;

namespace strongstore
{

    Client::Client(Mode mode, string configPath, int nShards,
                   int closestReplica, Partitioner *part, TrueTime timeServer)
        : transport(0.0, 0.0, 0), mode(mode), part(part), timeServer(timeServer)
    {
        // Initialize all state here;
        client_id = 0;
        while (client_id == 0)
        {
            random_device rd;
            mt19937_64 gen(rd());
            uniform_int_distribution<uint64_t> dis;
            client_id = dis(gen);
        }
        t_id = (client_id / 10000) * 10000;

        nshards = nShards;
        bclient.reserve(nshards);

        Debug("Initializing SpanStore client with id [%lu]", client_id);

        /* Start a client for time stamp server. */
        if (mode == MODE_OCC)
        {
            // TODO: Fix this config path
            string tssConfigPath = configPath + ".tss.config";
            ifstream tssConfigStream(tssConfigPath);
            if (tssConfigStream.fail())
            {
                fprintf(stderr, "unable to read configuration file: %s\n",
                        tssConfigPath.c_str());
            }
            transport::Configuration tssConfig(tssConfigStream);
            tss = new replication::vr::VRClient(tssConfig, &transport, 0, client_id);
        }

        /* Start a client for each shard. */
        ifstream configStream(configPath);
        if (configStream.fail())
        {
            fprintf(stderr, "unable to read configuration file: %s\n",
                    configPath.c_str());
        }
        transport::Configuration config(configStream);
        for (int i = 0; i < nShards; i++)
        {
            ShardClient *shardclient = new ShardClient(mode, &config,
                                                       &transport, client_id, i, closestReplica);
            bclient[i] = new BufferClient(shardclient);
        }

        /* Run the transport in a new thread. */
        clientTransport = new thread(&Client::run_client, this);

        Debug("SpanStore client [%lu] created!", client_id);
    }

    Client::~Client()
    {
        transport.Stop();
        delete tss;
        for (auto b : bclient)
        {
            delete b;
        }
        clientTransport->join();
    }

    /* Runs the transport event loop. */
    void
    Client::run_client()
    {
        transport.Run();
    }

    /* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
    void
    Client::Begin()
    {
        Debug("BEGIN Transaction");
        t_id++;
        participants.clear();
        commit_sleep = -1;
        for (int i = 0; i < nshards; i++)
        {
            bclient[i]->Begin(t_id);
        }
    }

    /* Returns the value corresponding to the supplied key. */
    int
    Client::Get(const string &key, string &value)
    {
        // Contact the appropriate shard to get the value.
        std::vector<int> txnGroups(participants.begin(), participants.end());
        int i = (*part)(key, nshards, -1, txnGroups);

        // If needed, add this shard to set of participants and send BEGIN.
        if (participants.find(i) == participants.end())
        {
            participants.insert(i);
        }

        // Send the GET operation to appropriate shard.
        Promise promise(GET_TIMEOUT);

        bclient[i]->Get(key, &promise);
        value = promise.GetValue();

        return promise.GetReply();
    }

    /* Sets the value corresponding to the supplied key. */
    int
    Client::Put(const string &key, const string &value)
    {
        // Contact the appropriate shard to set the value.
        std::vector<int> txnGroups(participants.begin(), participants.end());
        int i = (*part)(key, nshards, -1, txnGroups);

        // If needed, add this shard to set of participants and send BEGIN.
        if (participants.find(i) == participants.end())
        {
            participants.insert(i);
        }

        Promise promise(PUT_TIMEOUT);

        // Buffering, so no need to wait.
        bclient[i]->Put(key, value, &promise);
        return promise.GetReply();
    }

    int
    Client::ChooseCoordinator(const std::set<int> &participants)
    {
        ASSERT(participants.size() != 0);
        // TODO: Choose coordinator
        return *participants.begin();
    }

    int
    Client::Prepare(uint64_t &ts)
    {
        int status;

        // 1. Send commit-prepare to all shards.
        Debug("PREPARE Transaction");
        Promise promise(PREPARE_TIMEOUT);

        int coordShard = ChooseCoordinator(participants);
        int nParticipants = participants.size();

        for (auto p : participants)
        {
            Debug("Sending prepare to shard [%d]", p);
            if (p == coordShard)
            {
                bclient[p]->Prepare(coordShard, nParticipants, &promise);
            }
            else
            {
                bclient[p]->Prepare(coordShard, nParticipants);
            }
        }

        // 2. Wait for reply from all shards. (abort on timeout)
        Debug("Waiting for PREPARE replies");

        status = promise.GetReply();
        return status;
    }

    /* Attempts to commit the ongoing transaction. */
    bool
    Client::Commit()
    {
        // Implementing 2 Phase Commit
        uint64_t ts = 0;
        int status = Prepare(ts);

        if (status == REPLY_OK)
        {
            return true;
        }
        else
        {
            return false;
        }

        // for (int i = 0; i < COMMIT_RETRIES; i++) {
        //     status = Prepare(ts);
        //     if (status == REPLY_OK || status == REPLY_FAIL) {
        //         break;
        //     }
        // }

        // if (status == REPLY_OK) {
        //     // For Spanner like systems, calculate timestamp.
        //     if (mode == MODE_SPAN_OCC || mode == MODE_SPAN_LOCK) {
        //         uint64_t now, err;
        //         struct timeval t1, t2;

        //         gettimeofday(&t1, NULL);
        //         timeServer.GetTimeAndError(now, err);

        //         if (now > ts) {
        //             ts = now;
        //         } else {
        //             uint64_t diff = ((ts >> 32) - (now >> 32))*1000000 +
        //                     ((ts & 0xffffffff) - (now & 0xffffffff));
        //             err += diff;
        //         }

        //         commit_sleep = (int)err;

        //         // how good are we at waking up on time?
        //         Debug("Commit wait sleep: %lu", err);
        //         if (err > 1000000)
        //             Warning("Sleeping for too long! %lu; now,ts: %lu,%lu", err, now, ts);
        //         if (err > 150) {
        //             usleep(err-150);
        //         }
        //         // fine grained busy-wait
        //         while (1) {
        //             gettimeofday(&t2, NULL);
        //             if ((t2.tv_sec-t1.tv_sec)*1000000 +
        //                 (t2.tv_usec-t1.tv_usec) > (int64_t)err) {
        //                 break;
        //             }
        //         }
        //     }

        //     // Send commits
        //     Debug("COMMIT Transaction at [%lu]", ts);

        //     for (auto p : participants) {
        //         Debug("Sending commit to shard [%d]", p);
        //         bclient[p]->Commit(ts);
        //     }
        //     return true;
        // }

        // 4. If not, send abort to all shards.
        // Abort();
        // return false;
    }

    /* Aborts the ongoing transaction. */
    void
    Client::Abort()
    {
        Debug("ABORT Transaction");
        NOT_REACHABLE();
    }

    /* Return statistics of most recent transaction. */
    vector<int>
    Client::Stats()
    {
        vector<int> v;
        return v;
    }

    /* Callback from a tss replica upon any request. */
    void
    Client::tssCallback(const string &request, const string &reply)
    {
        lock_guard<mutex> lock(cv_m);
        Debug("Received TSS callback [%s]", reply.c_str());

        // Copy reply to "replica_reply".
        replica_reply = reply;

        // Wake up thread waiting for the reply.
        cv.notify_all();
    }

} // namespace strongstore
