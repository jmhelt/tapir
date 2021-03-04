// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include <csignal>

#include <valgrind/callgrind.h>

#include "lib/transport.h"
#include "lib/tcptransport.h"
#include "lib/udptransport.h"
#include "lib/io_utils.h"

#include "store/common/partitioner.h"
#include "store/server.h"
#include "store/strongstore/server.h"
#include "store/tapirstore/server.h"
#include "store/weakstore/server.h"

#include <gflags/gflags.h>

enum protocol_t {
	PROTO_UNKNOWN,
	PROTO_TAPIR,
	PROTO_WEAK,
	PROTO_STRONG
};

enum transmode_t {
	TRANS_UNKNOWN,
  TRANS_UDP,
  TRANS_TCP,
};

/**
 * System settings.
 */
DEFINE_string(config_path, "", "path to replication configuration file");
DEFINE_uint64(replica_idx, 0, "index of replica in replication configuration file");
DEFINE_uint64(group_idx, 0, "index of the shard to which this replica belongs");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_bool(debug_stats, false, "record stats related to debugging");

const std::string protocol_args[] = {
	"tapir",
  "weak",
  "strong",
};
const protocol_t protos[] {
  PROTO_TAPIR,
  PROTO_WEAK,
  PROTO_STRONG,
};
static bool ValidateProtocol(const char* flagname,
    const std::string &value) {
  int n = sizeof(protocol_args);
  for (int i = 0; i < n; ++i) {
    if (value == protocol_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(protocol, protocol_args[0],	"the protocol to use during this"
    " experiment");
DEFINE_validator(protocol, &ValidateProtocol);

const std::string trans_args[] = {
  "udp",
	"tcp"
};

const transmode_t transmodes[] {
  TRANS_UDP,
	TRANS_TCP
};
static bool ValidateTransMode(const char* flagname,
    const std::string &value) {
  int n = sizeof(trans_args);
  for (int i = 0; i < n; ++i) {
    if (value == trans_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(trans_protocol, trans_args[0], "transport protocol to use for"
		" passing messages");
DEFINE_validator(trans_protocol, &ValidateTransMode);

const std::string partitioner_args[] = {
	"default",
  "warehouse_dist_items",
  "warehouse"
};
const partitioner_t parts[] {
  DEFAULT,
  WAREHOUSE_DIST_ITEMS,
  WAREHOUSE
};
static bool ValidatePartitioner(const char* flagname,
    const std::string &value) {
  int n = sizeof(partitioner_args);
  for (int i = 0; i < n; ++i) {
    if (value == partitioner_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(partitioner, partitioner_args[0],	"the partitioner to use during this"
    " experiment");
DEFINE_validator(partitioner, &ValidatePartitioner);

/**
 * TPCC settings.
 */
DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");

/**
 * TAPIR settings.
 */
DEFINE_bool(tapir_linearizable, true, "run TAPIR in linearizable mode");

/**
 * StrongStore settings.
 */
const std::string strongmode_args[] = {
	"lock",
  "occ",
  "span-lock",
  "span-occ"
};
const strongstore::Mode strongmodes[] {
  strongstore::Mode::MODE_LOCK,
  strongstore::Mode::MODE_OCC,
  strongstore::Mode::MODE_SPAN_LOCK,
  strongstore::Mode::MODE_SPAN_OCC,
};
static bool ValidateStrongMode(const char* flagname,
    const std::string &value) {
  int n = sizeof(strongmode_args);
  for (int i = 0; i < n; ++i) {
    if (value == strongmode_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(strongmode, strongmode_args[0],	"the protocol to use during this"
    " experiment");
DEFINE_validator(strongmode, &ValidateStrongMode);
DEFINE_int64(strong_max_dep_depth, -1, "maximum length of dependency chain"
    " [-1 is no maximum] (for StrongStore MVTSO)");

/**
 * Experiment settings.
 */
DEFINE_int32(clock_skew, 0, "difference between real clock and TrueTime");
DEFINE_int32(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to file for server stats");

/**
 * Benchmark settings.
 */
DEFINE_string(keys_path, "", "path to file containing keys in the system");
DEFINE_uint64(num_keys, 0, "number of keys to generate");
DEFINE_string(data_file_path, "", "path to file containing key-value pairs to be loaded");
DEFINE_bool(preload_keys, false, "load keys into server if generating keys");

Server *server = nullptr;
TransportReceiver *replica = nullptr;
::Transport *tport = nullptr;
Partitioner *part = nullptr;

void Cleanup(int signal);

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
           "runs a replica for a distributed replicated transaction\n"
"           processing system.");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  Notice("Starting server.");

  // parse replication configuration
  std::ifstream config_stream(FLAGS_config_path);
  if (config_stream.fail()) {
    std::cerr << "Unable to read configuration file: " << FLAGS_config_path
              << std::endl;
  }

  // parse protocol and mode
  protocol_t proto = PROTO_UNKNOWN;
  int numProtos = sizeof(protocol_args);
  for (int i = 0; i < numProtos; ++i) {
    if (FLAGS_protocol == protocol_args[i]) {
      proto = protos[i];
      break;
    }
  }

  // parse transport protocol
  transmode_t trans = TRANS_UNKNOWN;
  int numTransModes = sizeof(trans_args);
  for (int i = 0; i < numTransModes; ++i) {
    if (FLAGS_trans_protocol == trans_args[i]) {
      trans = transmodes[i];
      break;
    }
  }
  if (trans == TRANS_UNKNOWN) {
    std::cerr << "Unknown transport protocol." << std::endl;
    return 1;
  }

  transport::Configuration config(config_stream);

  if (FLAGS_replica_idx >= static_cast<uint64_t>(config.n)) {
    std::cerr << "Replica index " << FLAGS_replica_idx << " is out of bounds"
                 "; only " << config.n << " replicas defined" << std::endl;
  }

  if (proto == PROTO_UNKNOWN) {
    std::cerr << "Unknown protocol." << std::endl;
    return 1;
  }

  strongstore::Mode strongMode = strongstore::Mode::MODE_UNKNOWN;
  if (proto == PROTO_STRONG) {
    int numStrongModes = sizeof(strongmode_args);
    for (int i = 0; i < numStrongModes; ++i) {
      if (FLAGS_strongmode == strongmode_args[i]) {
        strongMode = strongmodes[i];
        break;
      }
    }
  }

  switch (trans) {
    case TRANS_TCP:
      tport = new TCPTransport(0.0, 0.0, 0, false);
      break;
    case TRANS_UDP:
      tport = new UDPTransport(0.0, 0.0, 0, false);
      break;
    default:
      NOT_REACHABLE();
  }

  // parse protocol and mode
  partitioner_t partType = DEFAULT;
  int numParts = sizeof(partitioner_args);
  for (int i = 0; i < numParts; ++i) {
    if (FLAGS_partitioner == partitioner_args[i]) {
      partType = parts[i];
      break;
    }
  }

  std::mt19937 unused;
  switch (partType) {
    case DEFAULT:
      part = new DefaultPartitioner();
      break;
    case WAREHOUSE_DIST_ITEMS:
      part = new WarehouseDistItemsPartitioner(FLAGS_tpcc_num_warehouses);
      break;
    case WAREHOUSE:
      part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, unused);
      break;
    default:
      NOT_REACHABLE();
  }

  switch (proto) {
    case PROTO_TAPIR: {
      server = new tapirstore::Server(FLAGS_tapir_linearizable);
      replica = new replication::ir::IRReplica(config, FLAGS_group_idx, FLAGS_replica_idx,
          tport, dynamic_cast<replication::ir::IRAppReplica *>(server));
      break;
    }
    case PROTO_WEAK: {
      server = new weakstore::Server(config, FLAGS_group_idx, FLAGS_replica_idx, tport, new weakstore::Store());
      break;
    }
    case PROTO_STRONG: {
      strongstore::InterShardClient *shardClient = new strongstore::InterShardClient(config, tport, FLAGS_num_shards);
      server = new strongstore::Server(*shardClient, FLAGS_group_idx,
          FLAGS_replica_idx, strongMode, FLAGS_clock_skew,
          FLAGS_clock_error);
      replica = new replication::vr::VRReplica(config, FLAGS_group_idx,
          FLAGS_replica_idx, tport, 1,
          dynamic_cast<replication::AppReplica *>(server));
      break;
    }
    default: {
      NOT_REACHABLE();
    }
  }

  // parse keys
  size_t loaded = 0;
  size_t stored = 0;
  std::vector<int> txnGroups;
  std::vector<std::string> keys;
  if (FLAGS_data_file_path.empty() && FLAGS_keys_path.empty()) {
    if (FLAGS_num_keys > 0) {
      if (FLAGS_preload_keys) {
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
          string key = std::to_string(i);
          string value = "";
          if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) == FLAGS_group_idx) {
            if (i % 10000 == 0) {
              Debug("Loaded key %s", key.c_str());
            }
            server->Load(key, value, Timestamp());
            ++stored;
          }
          ++loaded;
        }
      }

      Debug("Stored %lu out of %lu key-value pairs from [0,%lu).", stored, loaded, FLAGS_num_keys);
    } else {
      std::cerr << "Specified neither keys file nor number of keys."
                << std::endl;
      return 1;
    }
  } else if (FLAGS_data_file_path.length() > 0 && FLAGS_keys_path.empty()) {
    std::ifstream in;
    in.open(FLAGS_data_file_path);
    if (!in) {
      std::cerr << "Could not read data from: " << FLAGS_data_file_path
                << std::endl;
      return 1;
    }

    Debug("Populating with data from %s.", FLAGS_data_file_path.c_str());
    while (!in.eof()) {
      std::string key;
      std::string value;
      int i = ReadBytesFromStream(&in, key);
      if (i == 0) {
        ReadBytesFromStream(&in, value);
        if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) == FLAGS_group_idx) {
          server->Load(key, value, Timestamp());
          ++stored;
        }
        ++loaded;
      }
    }
    Debug("Stored %lu out of %lu key-value pairs from file %s.", stored,
        loaded, FLAGS_data_file_path.c_str());
  } else {
    std::ifstream in;
    in.open(FLAGS_keys_path);
    if (!in) {
      std::cerr << "Could not read keys from: " << FLAGS_keys_path
                << std::endl;
      return 1;
    }
    std::string key;
    std::vector<int> txnGroups;
    while (std::getline(in, key)) {
      if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) == FLAGS_group_idx) {
        server->Load(key, "", Timestamp(0, 0));
      }
    }
    in.close();
  }
  Notice("Done loading server.");

  std::signal(SIGKILL, Cleanup);
  std::signal(SIGTERM, Cleanup);
  std::signal(SIGINT, Cleanup);

  CALLGRIND_START_INSTRUMENTATION;
  tport->Run();
  CALLGRIND_STOP_INSTRUMENTATION;
  CALLGRIND_DUMP_STATS;

  if (FLAGS_stats_file.size() > 0) {
    Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
    server->GetStats().ExportJSON(FLAGS_stats_file);
  }

  return 0;
}

void Cleanup(int signal) {
  Notice("Gracefully exiting after signal %d.", signal);
  if (FLAGS_stats_file.size() > 0) {
    Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
    server->GetStats().ExportJSON(FLAGS_stats_file);
  }
  delete server;
  exit(0);
}
