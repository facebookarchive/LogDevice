/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ServerSettings.h"

#include <boost/program_options.hpp>
#include <folly/String.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/settings/Validators.h"

using namespace facebook::logdevice::setting_validators;

namespace facebook { namespace logdevice {

// maximum allowed number of storage threads to run
#define STORAGE_THREADS_MAX 10000

static void validate_storage_threads(const char* name, int value, int min) {
  if (value < min || value > STORAGE_THREADS_MAX) {
    char buf[1024];
    snprintf(buf,
             sizeof(buf),
             "Invalid value for --%s: %d. Must be between %d and %d.",
             name,
             value,
             min,
             STORAGE_THREADS_MAX);
    throw boost::program_options::error(buf);
  }
}

static SequencerOptions validate_sequencers(const std::string& value) {
  if (value == "all") {
    return SequencerOptions::ALL;
  } else if (value == "lazy") {
    return SequencerOptions::LAZY;
  } else if (value == "none") {
    return SequencerOptions::NONE;
  } else {
    throw boost::program_options::error("Invalid value for -S: " + value +
                                        ". Expected one of 'all', "
                                        "'lazy'.");
  }
}

void ServerSettings::defineSettings(SettingEasyInit& init) {
  using namespace SettingFlag;
  using ThreadType = StorageTaskThreadType;

  // clang-format off

  init
    ("unix-socket", &unix_socket, "", validate_unix_socket,
     "Path to the unix domain socket the server will use to listen for "
     "non-SSL clients",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::Testing)

    ("port", &port, "16111", validate_port,
     "TCP port on which the server listens for non-SSL clients",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::Core)

    ("ssl-unix-socket", &deprecated_ssl_unix_socket, "", nullptr,
     "Deprecated and ignored.",
     SERVER | REQUIRES_RESTART | DEPRECATED)

    ("ssl-port", &deprecated_ssl_port, "0", nullptr,
     "Deprecated and ignored.",
     SERVER | REQUIRES_RESTART | DEPRECATED)

    ("command-unix-socket", &command_unix_socket, "", validate_unix_socket,
     "Path to the unix domain socket the server will use to listen for admin "
     "commands, supports commands over SSL",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Testing)

    ("command-port", &command_port, "5440", validate_port,
     "TCP port on which the server listens to for admin commands, supports "
     "commands over SSL",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Core)

    ("require-ssl-on-command-port", &require_ssl_on_command_port, "false",
     nullptr, "Requires SSL for admin commands sent to the command port. "
     "--ssl-cert-path, --ssl-key-path and --ssl-ca-path settings must be "
     "properly configured",
     SERVER | EXPERIMENTAL,
     SettingsCategory::Security)

    ("admin-enabled", &admin_enabled, "true", nullptr,
     "Is Admin API enabled?",
     SERVER | REQUIRES_RESTART | EXPERIMENTAL,
     SettingsCategory::Core)

    ("command-conn-limit", &command_conn_limit, "32",
     [](int x) -> void {
       if (x <= 0) {
         throw boost::program_options::error(
           "command-conn-limit should be a positive integer"
         );
       }
     },
     "Maximum number of concurrent admin connections",
     SERVER,
     SettingsCategory::Network)

    ("loglevel",
     &loglevel,
     "info",
     parse_log_level,
     "One of the following: critical, error, warning, info, debug, none",
     SERVER,
     SettingsCategory::Core)

    ("external-loglevel",
     &external_loglevel,
     "critical",
     parse_log_level,
     "One of the following: critical, error, warning, info, debug, none",
     SERVER,
     SettingsCategory::Core)

    ("loglevel-overrides", &loglevel_overrides, "",
     [](const std::string& val) {
      dbg::LogLevelMap loglevels;
      std::vector<std::string> configs;
      folly::split(",", val, configs, true);
      for (auto v: configs) {
        std::vector<std::string> items;
        folly::split(":", v, items);
        if (items.size() != 2) {
          auto error_str = folly::format("Invalid value for "
                                         "--loglevel-overrides: {}. "
                                         "Expected comma-separated list of "
                                         "<module>:<loglevel>",
                                         val).str();
          throw boost::program_options::error(error_str);
        }
        dbg::Level level = dbg::parseLoglevel(items[1].c_str());
        if (level == dbg::Level::NONE) {
          auto error_str = folly::format("Invalid value for "
                                         "--loglevel-overrides: {}. "
                                         "Expected levels to be one of: "
                                         "critical, error, warning, "
                                         "notify, info, debug, spew",
                                         val).str();
          throw boost::program_options::error(error_str);
        }
        loglevels[items[0]] = level;
      }
      return loglevels;
    },
     "Comma-separated list of <module>:<loglevel>. eg: "
     "Server.cpp:debug,Sequencer.cpp:notify",
     SERVER,
     SettingsCategory::Testing)

    ("num-background-workers",
     &num_background_workers,
     "4",
     parse_nonnegative<ssize_t>(),
     "The number of workers dedicated for processing time-insensitive "
     "requests and operations",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Execution)

    ("assert-on-data", &assert_on_data, "false",
     nullptr,
     "Trigger asserts on data in RocksDB (or that received from the network). "
     "Should not be used in prod.",
     SERVER,
     SettingsCategory::Testing)

    ("log-file", &log_file, "",
     nullptr,
     "write server error log to specified file instead of stderr",
     SERVER,
     SettingsCategory::Core)

    // TODO: this option is required.
    ("config-path", &config_path, "",
     nullptr,
     "location of the cluster config file to use. Format: "
     "[file:]<path-to-config-file> or configerator:<configerator-path>",
     SERVER | CLI_ONLY | REQUIRES_RESTART,
     SettingsCategory::Configuration)


    ("storage-threads-per-shard-slow",
     &storage_pool_params[(size_t)ThreadType::SLOW].nthreads,
     "2",
     [](int val) {
      validate_storage_threads("storage-threads-per-shard-slow", val, 1);
     },
     "size of the 'slow' storage thread pool, per shard. This storage "
     "thread pool executes storage tasks that read log records from RocksDB, "
     "both to serve read requests from clients, and for rebuilding. Those are "
     "likely to block on IO.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Storage)

    ("storage-threads-per-shard-fast",
     &storage_pool_params[
       (size_t)ThreadType::FAST_TIME_SENSITIVE].nthreads,
     "2",
     [](int val) {
       validate_storage_threads("storage-threads-per-shard-fast", val, 0);
     },
     "size of the 'fast' storage thread pool, per shard. This storage thread "
     "pool executes storage tasks that write into RocksDB. Such tasks normally "
     "do not block on IO. If zero, slow threads will handle write tasks.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Storage)

    ("storage-threads-per-shard-fast-stallable",
     &storage_pool_params[(size_t)ThreadType::FAST_STALLABLE].nthreads,
     "1",
     [](int val) {
       validate_storage_threads("storage-threads-per-shard-fast-stallable",
                                val, 0);
     },
     "size of the thread pool (per shard) executing low priority write tasks, "
     "such as writing rebuilding records into RocksDB. Measures are taken to "
     "not schedule low-priority writes on this thread pool when there is work "
     "for 'fast' threads. If zero, normal fast threads will handle low-pri "
     "write tasks",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Storage)

    ("storage-threads-per-shard-default",
     &storage_pool_params[(size_t)ThreadType::DEFAULT].nthreads,
     "2",
     [](int val) {
       validate_storage_threads("storage-threads-per-shard-default", val, 0);
     },
     "size of the storage thread pool for small client requests and metadata "
     "operations, per shard. If zero, the 'slow' pool will be used for such "
     "tasks. ",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Storage)

     // For backward compatibility
    ("storage-threads-per-shard",
     &storage_pool_params[(size_t)ThreadType::SLOW].nthreads,
     "4",
     [](int val) {
      validate_storage_threads("storage-threads-per-shard", val, 1);
     },
     "same as --storage-threads-per-shard-slow",
     SERVER | REQUIRES_RESTART | DEPRECATED,
     SettingsCategory::Storage)

    ("epoch-store-path", &epoch_store_path, "", nullptr,
     "directory containing epoch files for logs (for testing only)",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Testing)

    ("shutdown-timeout", &shutdown_timeout, "120s",
     [](std::chrono::milliseconds val) -> void {
       if (val.count() <= 0) {
         throw boost::program_options::error(
           "shutdown-timeout must be positive"
         );
       }
     },
     "amount of time to wait for the server to shut down before terminating "
     "the process. Consider modifying --time-delay-before-force-abort when "
     "changing this value.",
     SERVER,
     SettingsCategory::Core)

    ("storage-thread-delaying-sync-interval",
     &storage_thread_delaying_sync_interval,
     "100ms",
     validate_nonnegative<ssize_t>(),
     "Interval between invoking syncs for delayable storage tasks. "
     "Ignored when undelayable task is being enqueued.",
     SERVER,
     SettingsCategory::Storage)

    ("fd-limit", &fd_limit, "0",
     [](int val) -> void {
       if (val < 0) {
         throw boost::program_options::error(
           "fd-limit must be non-negative"
         );
       }
     },
     "maximum number of file descriptors that the process can allocate (may "
     "require root priviliges). If equal to zero, do not set any limit.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::ResourceManagement)

    ("num-reserved-fds", &num_reserved_fds, "0",
     [](int val) -> void {
       if (val < 0) {
         throw boost::program_options::error(
           "reserved-fds must be non-negative"
         );
       }
     },
     "expected number of file descriptors to reserve for use by RocksDB files "
     "and server-to-server connections within the cluster. This number is "
     "subtracted from --fd-limit (if set) to obtain the maximum number of "
     "client TCP connections that the server will be willing to accept. ",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::ResourceManagement)

    ("eagerly-allocate-fdtable", &eagerly_allocate_fdtable, "false", nullptr,
     "enables an optimization to eagerly allocate the kernel fdtable "
     "at startup",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::ResourceManagement)

    ("lock-memory", &lock_memory, "false", nullptr,
     "On startup, call mlockall() to lock the text segment (executable code) "
     "of logdeviced in RAM.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::ResourceManagement)

    ("user", &user, "", nullptr,
     "user to switch to if server is run as root",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Core)

    ("sequencers", &sequencer, "lazy", validate_sequencers,
     "one of the following: all (run sequencers for all logs), "
     "lazy (bring them up as needed). Deprecated, do not use.",
     SERVER | REQUIRES_RESTART | DEPRECATED)

    ("server-id", &server_id, "", nullptr,
     "optional server ID, reported by INFO admin command",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Core)

    ("unmap-caches", &unmap_caches, "true", nullptr,
     "unmap RocksDB block cache before dumping core (reduces core file "
     "size)",
     SERVER,
     SettingsCategory::Core)

    ("disable-event-log-trimming", &disable_event_log_trimming, "false",
     nullptr,
     "Disable trimming of the event log (for tests only)",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Testing)

    ("ignore-cluster-marker", &ignore_cluster_marker, "false", nullptr,
     "If cluster marker is missing or doesn't match, overwrite it and carry "
     "on. Cluster marker is a file that LogsDB writes in the data directory "
     "of each shard to identify the shard id, node id, and cluster name to "
     "which the data in that directory belongs. Cluster marker mismatch "
     "indicates that a drive or node was moved to another cluster or "
     "another shard, and the data must not be used.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Storage)

    ("audit-log", &audit_log, "", nullptr,
     "Path for log file storing information about all trim point changes. "
     "For log rotation using logrotate send SIGHUP to process after rotation "
     "to reopen the log.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Security)

    ("connection-backlog", &connection_backlog, "2000",
     parse_positive<ssize_t>(),
     "(server-only setting) Maximum number of incoming connections that have "
     "been accepted by listener (have an open FD) but have not been processed "
     "by workers (made logdevice protocol handshake).",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::Network)

    ("shutdown-on-my-node-id-mismatch",
     &shutdown_on_my_node_id_mismatch,
     "true",
     nullptr,
     "Gracefully shutdown whenever the server's NodeID changes",
     SERVER,
     SettingsCategory::Configuration)

    ;
  // clang-format on

  init("test-mode",
       &test_mode,
       "false",
       nullptr,
       "Enable functionality in integration tests. Currently used for admin "
       "commands that are only enabled for testing purposes.",
       SERVER | CLI_ONLY | REQUIRES_RESTART,
       SettingsCategory::Execution);
}

}} // namespace facebook::logdevice
