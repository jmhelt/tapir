import collections
import ipaddress

from lib.experiment_codebase import *
from utils.experiment_util import *


class RssCodebase(ExperimentCodebase):

    def get_client_cmd(self, config, i, k, run, local_exp_directory,
                       remote_exp_directory):

        client = config["clients"][i]
        name, _ = os.path.splitext(config['network_config_file_name'])
        if 'run_locally' in config and config['run_locally']:
            path_to_client_bin = os.path.join(config['src_directory'],
                                              config['bin_directory_name'], config['client_bin_name'])
            exp_directory = local_exp_directory
            replica_config_path = os.path.join(
                local_exp_directory, config["replica_config"])
            shard_config_path = os.path.join(
                local_exp_directory, config["shard_config"])
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'], client,
                                      '%s-%d-stats-%d.json' % (client, k, run))
        else:
            path_to_client_bin = os.path.join(
                config['base_remote_bin_directory_nfs'],
                config['bin_directory_name'], config['client_bin_name'])
            exp_directory = remote_exp_directory
            replica_config_path = os.path.join(
                remote_exp_directory, config["replica_config"])
            shard_config_path = os.path.join(
                remote_exp_directory, config["shard_config"])
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      '%s-%d-stats-%d.json' % (client, k, run))

        client_threads = config["client_threads_per_process"] if "client_threads_per_process" in config else 1

        client_id = i * len(config["clients"]) * \
            config["client_processes_per_client_node"] + k

        truetime_error = config["truetime_error"] if "truetime_error" in config else 0
        client_command = ' '.join([str(x) for x in [
            path_to_client_bin,
            '--client_id', client_id,
            '--replica_config_path', replica_config_path,
            '--shard_config_path', shard_config_path,
            '--num_shards', config['num_shards'],
            '--benchmark', config['benchmark_name'],
            '--exp_duration', config['client_experiment_length'],
            '--warmup_secs', config['client_ramp_up'],
            '--cooldown_secs', config['client_ramp_down'],
            '--protocol_mode', config['client_protocol_mode'],
            '--stats_file', stats_file,
            '--num_clients', client_threads,
            '--clock_error', truetime_error]])

        if config['server_emulate_wan']:
            client_command += ' --ping_replicas=true'

        if config['replication_protocol'] == 'tapir':
            if 'sync_commit' in config['replication_protocol_settings']:
                client_command += ' --tapir_sync_commit=%s' % (
                    str(config['replication_protocol_settings']['sync_commit']).lower())

        if config['replication_protocol'] == 'strong':
            if 'unreplicated' in config['replication_protocol_settings']:
                client_command += ' --strong_unreplicated=%s' % str(
                    config['replication_protocol_settings']['unreplicated']).lower()

        if 'message_transport_type' in config['replication_protocol_settings']:
            client_command += ' --trans_protocol %s' % config['replication_protocol_settings']['message_transport_type']

        if 'client_debug_stats' in config and config['client_debug_stats']:
            client_command += ' --debug_stats'

        if 'client_message_timeout' in config:
            client_command += ' --message_timeout %d' % config['client_message_timeout']
        if 'client_abort_backoff' in config:
            client_command += ' --abort_backoff %d' % config['client_abort_backoff']
        if 'client_retry_aborted' in config:
            client_command += ' --retry_aborted=%s' % (
                str(config['client_retry_aborted']).lower())
        if 'client_max_attempts' in config:
            client_command += ' --max_attempts %d' % config['client_max_attempts']
        if 'client_max_backoff' in config:
            client_command += ' --max_backoff %d' % config['client_max_backoff']
        if 'client_rand_sleep' in config:
            client_command += ' --delay %d' % config['client_rand_sleep']

        if 'partitioner' in config:
            client_command += ' --partitioner %s' % config['partitioner']

        if config['benchmark_name'] == 'retwis':
            client_command += ' --num_keys %d' % config['client_num_keys']
            if 'client_key_selector' in config:
                client_command += ' --key_selector %s' % config['client_key_selector']
            if config['client_key_selector'] == 'zipf':
                client_command += ' --zipf_coefficient %f' % config['client_zipf_coefficient']

        if 'client_wrap_command' in config and len(config['client_wrap_command']) > 0:
            client_command = config['client_wrap_command'] % client_command

        if 'run_locally' in config and config['run_locally']:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       client,
                                       '%s-%d-stdout-%d.log' % (client, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       client,
                                       '%s-%d-stderr-%d.log' % (client, k, run))

            client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                 stderr_file)
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       '%s-%d-stdout-%d.log' % (client, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       '%s-%d-stderr-%d.log' % (client, k, run))
            if 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
                client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                     stderr_file)
            else:
                client_command = tcsh_redirect_output_to_files(client_command,
                                                               stdout_file, stderr_file)

        if isinstance(config['client_debug_output'], str) or config['client_debug_output']:
            if 'run_locally' in config and config['run_locally'] or 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
                if isinstance(config['client_debug_output'], str):
                    client_command = 'DEBUG=%s %s' % (
                        config['client_debug_output'], client_command)
                else:
                    client_command = 'DEBUG=all %s' % client_command
            else:
                if isinstance(config['client_debug_output'], str):
                    client_command = 'setenv DEBUG %s; %s' % (
                        config['client_debug_output'], client_command)
                else:
                    client_command = 'setenv DEBUG all; %s' % client_command

        client_command = '(cd %s; %s) & ' % (exp_directory, client_command)
        return client_command

    def get_replica_cmd(self, config, shard_idx, replica_idx, run, local_exp_directory,
                        remote_exp_directory):
        name, ext = os.path.splitext(config['network_config_file_name'])
        if 'run_locally' in config and config['run_locally']:
            path_to_server_bin = os.path.join(config['src_directory'],
                                              config['bin_directory_name'], config['server_bin_name'])
            exp_directory = local_exp_directory
            replica_config_path = os.path.join(
                local_exp_directory, config["replica_config"])
            shard_config_path = os.path.join(
                local_exp_directory, config["shard_config"])
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'], 'server-%d' % shard_idx,
                                      'server-%d-%d-stats-%d.json' % (shard_idx, replica_idx, run))
        else:
            path_to_server_bin = os.path.join(
                config['base_remote_bin_directory_nfs'],
                config['bin_directory_name'], config['server_bin_name'])
            exp_directory = remote_exp_directory
            replica_config_path = os.path.join(
                remote_exp_directory, config["replica_config"])
            shard_config_path = os.path.join(
                remote_exp_directory, config["shard_config"])
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      'server-%d-%d-stats-%d.json' % (shard_idx, replica_idx, run))

        n = 2 * config['fault_tolerance'] + 1
        server_id = shard_idx * n + replica_idx

        truetime_error = config["truetime_error"] if "truetime_error" in config else 0
        replica_command = ' '.join([str(x) for x in [
            path_to_server_bin,
            '--server_id', server_id,
            '--replica_config_path', replica_config_path,
            '--shard_config_path', shard_config_path,
            '--group_idx', shard_idx,
            '--replica_idx', replica_idx,
            '--protocol', config['replication_protocol'],
            '--num_shards', config['num_shards'],
            '--stats_file', stats_file,
            '--clock_error', truetime_error]])

        if 'message_transport_type' in config['replication_protocol_settings']:
            replica_command += ' --trans_protocol %s' % config['replication_protocol_settings']['message_transport_type']

        if config['replication_protocol'] == 'strong':
            if 'strongmode' in config['replication_protocol_settings']:
                replica_command += ' --strongmode=%s' % str(
                    config['replication_protocol_settings']['strongmode'])
            if 'max_dep_depth' in config['replication_protocol_settings']:
                replica_command += ' --strong_max_dep_depth %d' % config['replication_protocol_settings']['max_dep_depth']
            if 'unreplicated' in config['replication_protocol_settings']:
                replica_command += ' --strong_unreplicated=%s' % str(
                    config['replication_protocol_settings']['unreplicated']).lower()

        if config['replication_protocol'] == 'tapir':
            if 'strictly_serializable' in config['replication_protocol_settings']:
                replica_command += ' --tapir_linearizable=%s' % str(
                    config['replication_protocol_settings']['strictly_serializable']).lower()

        if config['replication_protocol'] == 'morty' or config['replication_protocol'] == 'morty-context':
            if 'branch' in config['replication_protocol_settings']:
                replica_command += ' --morty_branch=%s' % str(
                    config['replication_protocol_settings']['branch']).lower()
            if 'prepare_delay_ms' in config['replication_protocol_settings']:
                replica_command += ' --morty_prepare_delay_ms %d' % config[
                    'replication_protocol_settings']['prepare_delay_ms']

        if config['replication_protocol'] == 'indicus' or config['replication_protocol'] == 'pbft' or config['replication_protocol'] == 'hotstuff':
            if 'read_dep' in config['replication_protocol_settings']:
                replica_command += ' --indicus_read_dep %s' % config['replication_protocol_settings']['read_dep']
            if 'watermark_time_delta' in config['replication_protocol_settings']:
                replica_command += ' --indicus_time_delta %d' % config[
                    'replication_protocol_settings']['watermark_time_delta']
            if 'sign_messages' in config['replication_protocol_settings']:
                replica_command += ' --indicus_sign_messages=%s' % str(
                    config['replication_protocol_settings']['sign_messages']).lower()
                replica_command += ' --indicus_key_path %s' % config['replication_protocol_settings']['key_path']
            if 'validate_proofs' in config['replication_protocol_settings']:
                replica_command += ' --indicus_validate_proofs=%s' % str(
                    config['replication_protocol_settings']['validate_proofs']).lower()
            if 'hash_digest' in config['replication_protocol_settings']:
                replica_command += ' --indicus_hash_digest=%s' % str(
                    config['replication_protocol_settings']['hash_digest']).lower()
            if 'verify_deps' in config['replication_protocol_settings']:
                replica_command += ' --indicus_verify_deps=%s' % str(
                    config['replication_protocol_settings']['verify_deps']).lower()
            if 'max_dep_depth' in config['replication_protocol_settings']:
                replica_command += ' --indicus_max_dep_depth %d' % config[
                    'replication_protocol_settings']['max_dep_depth']
            if 'signature_type' in config['replication_protocol_settings']:
                replica_command += ' --indicus_key_type %d' % config['replication_protocol_settings']['signature_type']
            if 'sig_batch' in config['replication_protocol_settings']:
                replica_command += ' --indicus_sig_batch %d' % config['replication_protocol_settings']['sig_batch']
            if 'sig_batch_timeout' in config['replication_protocol_settings']:
                replica_command += ' --indicus_sig_batch_timeout %d' % config[
                    'replication_protocol_settings']['sig_batch_timeout']
            if 'occ_type' in config['replication_protocol_settings']:
                replica_command += ' --indicus_occ_type %s' % config['replication_protocol_settings']['occ_type']
            if 'read_reply_batch' in config['replication_protocol_settings']:
                replica_command += ' --indicus_read_reply_batch=%s' % str(
                    config['replication_protocol_settings']['read_reply_batch']).lower()
            if 'adjust_batch_size' in config['replication_protocol_settings']:
                replica_command += ' --indicus_adjust_batch_size=%s' % str(
                    config['replication_protocol_settings']['adjust_batch_size']).lower()
            if 'shared_mem_batch' in config['replication_protocol_settings']:
                replica_command += ' --indicus_shared_mem_batch=%s' % str(
                    config['replication_protocol_settings']['shared_mem_batch']).lower()
            if 'shared_mem_verify' in config['replication_protocol_settings']:
                replica_command += ' --indicus_shared_mem_verify=%s' % str(
                    config['replication_protocol_settings']['shared_mem_batch']).lower()
            if 'merkle_branch_factor' in config['replication_protocol_settings']:
                replica_command += ' --indicus_merkle_branch_factor %d' % config[
                    'replication_protocol_settings']['merkle_branch_factor']
            if 'batch_tout' in config['replication_protocol_settings']:
                replica_command += ' --indicus_sig_batch_timeout %d' % config[
                    'replication_protocol_settings']['batch_tout']
            if 'batch_size' in config['replication_protocol_settings']:
                replica_command += ' --indicus_sig_batch %d' % config['replication_protocol_settings']['batch_size']
            if 'ebatch_tout' in config['replication_protocol_settings']:
                replica_command += ' --indicus_esig_batch_timeout %d' % config[
                    'replication_protocol_settings']['ebatch_tout']
            if 'ebatch_size' in config['replication_protocol_settings']:
                replica_command += ' --indicus_esig_batch %d' % config['replication_protocol_settings']['ebatch_size']
            if 'use_coord' in config['replication_protocol_settings']:
                replica_command += ' --indicus_use_coordinator=%s' % str(
                    config['replication_protocol_settings']['use_coord']).lower()
            # Added multithreading and batch verification
            if 'multi_threading' in config['replication_protocol_settings']:
                replica_command += ' --indicus_multi_threading=%s' % str(
                    config['replication_protocol_settings']['multi_threading']).lower()
            if 'batch_verification' in config['replication_protocol_settings']:
                replica_command += ' --indicus_batch_verification=%s' % str(
                    config['replication_protocol_settings']['batch_verification']).lower()
            if 'batch_verification_size' in config['replication_protocol_settings']:
                replica_command += ' --indicus_batch_verification_size %d' % config[
                    'replication_protocol_settings']['batch_verification_size']

        if 'server_debug_stats' in config and config['server_debug_stats']:
            replica_command += ' --debug_stats'

        if config['benchmark_name'] == 'retwis':
            replica_command += ' --num_keys %d' % config['client_num_keys']
            if 'server_preload_keys' in config:
                replica_command += ' --preload_keys=%s' % str(
                    config['server_preload_keys']).lower()
        elif config['benchmark_name'] == 'rw':
            replica_command += ' --num_keys %d' % config['client_num_keys']
            if 'server_preload_keys' in config:
                replica_command += ' --preload_keys=%s' % str(
                    config['server_preload_keys']).lower()
        elif config['benchmark_name'] == 'tpcc' or config['benchmark_name'] == 'tpcc-sync':
            replica_command += ' --data_file_path %s' % config['tpcc_data_file_path']
            replica_command += ' --tpcc_num_warehouses %d' % config['tpcc_num_warehouses']
        elif config['benchmark_name'] == 'smallbank':
            replica_command += ' --data_file_path %s' % config['smallbank_data_file_path']

        if 'partitioner' in config:
            replica_command += ' --partitioner %s' % config['partitioner']

        if 'server_wrap_command' in config and len(config['server_wrap_command']) > 0:
            replica_command = config['server_wrap_command'] % replica_command

        if 'pin_server_processes' in config and isinstance(config['pin_server_processes'], list) and len(config['pin_server_processes']) > 0:
            core = config['pin_server_processes'][replica_idx %
                                                  len(config['pin_server_processes'])]
            replica_command = 'taskset 0x%x %s' % (1 << core, replica_command)

        # Wrapping additional information around command
        if 'run_locally' in config and config['run_locally']:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'], 'server-%d' % shard_idx,
                                       'server-%d-%d-stdout-%d.log' % (shard_idx, replica_idx, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'], 'server-%d' % shard_idx,
                                       'server-%d-%d-stderr-%d.log' % (shard_idx, replica_idx, run))
            replica_command = '%s 1> %s 2> %s' % (replica_command, stdout_file,
                                                  stderr_file)
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'], 'server-%d-%d-stdout-%d.log' % (
                                           shard_idx, replica_idx, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'], 'server-%d-%d-stderr-%d.log' % (
                                           shard_idx, replica_idx, run))

            if 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
                replica_command = '%s 1> %s 2> %s' % (replica_command, stdout_file,
                                                      stderr_file)
            else:
                replica_command = tcsh_redirect_output_to_files(replica_command,
                                                                stdout_file, stderr_file)

        if isinstance(config['server_debug_output'], str) or config['server_debug_output']:
            if 'run_locally' in config and config['run_locally'] or 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
                if isinstance(config['server_debug_output'], str):
                    replica_command = 'DEBUG=%s %s' % (config['server_debug_output'],
                                                       replica_command)
                else:
                    replica_command = 'DEBUG=all %s' % replica_command
            else:
                if isinstance(config['server_debug_output'], str):
                    replica_command = 'setenv DEBUG %s; %s' % (
                        config['server_debug_output'], replica_command)
                else:
                    replica_command = 'setenv DEBUG all; %s' % replica_command
        replica_command = 'cd %s; %s' % (exp_directory, replica_command)
        return replica_command

    def prepare_local_exp_directory(self, config, config_file):
        local_exp_directory = super().prepare_local_exp_directory(config, config_file)
        server_names = config["server_names"]
        fault_tolerance = config["fault_tolerance"]
        n = 2 * fault_tolerance + 1
        shards = config["shards"]
        assert(len(shards) == config["num_shards"])
        server_base_port = config["server_port"]
        server_ports = collections.defaultdict(lambda: server_base_port)
        shard_idx = 0
        replica_config_path = os.path.join(
            local_exp_directory, config["replica_config"])
        shard_config_path = os.path.join(
            local_exp_directory, config["shard_config"])
        print(shard_config_path)
        with open(replica_config_path, "w") as rcf, open(shard_config_path, "w") as scf:
            print("f {}".format(fault_tolerance), file=rcf)
            print("f {}".format(fault_tolerance), file=scf)
            for shard in shards:
                print("group", file=rcf)
                print("group", file=scf)
                assert(len(shard) == n)
                for replica in shard:
                    assert(replica in server_names)
                    if "run_locally" in config and config["run_locally"]:
                        replica = "localhost"

                    port = server_ports[replica]
                    print("replica {}:{}".format(replica, port), file=rcf)
                    print("replica {}:{}".format(replica, port+1), file=scf)
                    server_ports[replica] += 2
            shard_idx += 1

        return local_exp_directory

    def prepare_remote_server_codebase(self, config, host, local_exp_directory, remote_out_directory):
        if config['replication_protocol'] == 'indicus' or config['replication_protocol'] == 'hotstuff':
            run_remote_command_sync(
                'sudo rm -rf /dev/shm/*', config['emulab_user'], host)

    def setup_nodes(self, config):
        pass
