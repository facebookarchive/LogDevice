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

namespace {
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

static configuration::nodes::RoleSet parse_roles(const std::string& value) {
  std::vector<std::string> role_strs;
  configuration::nodes::RoleSet roles;
  folly::split(",", value, role_strs);
  for (const auto& role : role_strs) {
    if (role == "sequencer") {
      roles[static_cast<size_t>(configuration::nodes::NodeRole::SEQUENCER)] =
          true;
    } else if (role == "storage") {
      roles[static_cast<size_t>(configuration::nodes::NodeRole::STORAGE)] =
          true;
    } else {
      throw boost::program_options::error(
          folly::sformat("Invalid role: {}", role));
    }
  }
  return roles;
}

static NodeLocation parse_location(const std::string& value) {
  auto loc = NodeLocation{};
  if (value.empty()) {
    return loc;
  }
  auto success = loc.fromDomainString(value);
  if (success != 0) {
    throw boost::program_options::error(
        folly::sformat("Invalid location {}: {} {}.",
                       value,
                       error_name(err),
                       error_description(err)));
  }
  return loc;
}

} // namespace

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
     "require root privileges). If equal to zero, do not set any limit.",
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

    ("shutdown-on-node-configuration-mismatch",
     &shutdown_on_node_configuration_mismatch,
     "true",
     nullptr,
     "Gracefully shutdown whenever the server's NodeID and/or service discovery "
     "configuration changes.",
     SERVER,
     SettingsCategory::Configuration)

    ("enable-node-self-registration", &enable_node_self_registration, "false",
     nullptr,
     "If set, the node will register itself in the config if it doesn't find "
     "itself there. Otherwise it will crash. This requires "
     "--enable-nodes-configuration-manager=true",
     SERVER | REQUIRES_RESTART | EXPERIMENTAL,
     SettingsCategory::Configuration)

    ("name", &name, "",
     nullptr,
     "The name that the server will use to self register in the nodes configuration. "
     "This is the main identifier that the node uses to join the cluster. At "
     "any point of time, all the names in the config are unique. If a node joins "
     "with a name that's used by another running node, the new node will preempt "
     "the old one.",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("node-version", &version, "",
     [](const std::string& val) -> decltype(version) {
       using value_type = typename std::decay<decltype(*version)>::type;
       if (val.empty()) {
         return folly::none;
       }
       return parse_nonnegative<value_type>()("node-version", val);
     },
     "[Only used when node self registration is enabled] The version provides "
     "better control over node self-registration logic. A node will only be "
     "allowed to update its attributes on joining the cluster its version is "
     "greater or equal than the current one. When omitted, will default to "
     "the current version if there is another node running with the same "
     "name, and to 0 otherwise.",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("address", &address, "",
     nullptr,
     "[Only used when node self registration is enabled] The interface address "
     "that the server will be listening on for data, gossip, command and admin"
     " connections (unless overridden by unix sockets"
     " settings).",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("gossip-unix-socket", &gossip_unix_socket, "", validate_unix_socket,
     "[Only used when node self registration is enabled] Path to the unix "
     "domain socket the server will use to listen for "
     "gossip connections",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("gossip-port", &gossip_port, "0", validate_optional_port,
     "[Only used when node self registration is enabled] TCP port on which the "
     "server listens for gossip connections. A value of "
     "zero means that the server will listen for socket on the data port.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("ssl-unix-socket", &ssl_unix_socket, "", validate_unix_socket,
     "[Only used when node self registration is enabled] Path to the unix "
     "domain socket the server will use to listen for SSL clients",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("ssl-port", &ssl_port, "0", validate_optional_port,
     "[Only used when node self registration is enabled] TCP port on which the "
     "server listens for SSL clients. A value of zero "
     "means that the server won't listen for SSL connections.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("server-to-server-unix-socket", &server_to_server_unix_socket, "", validate_unix_socket,
     "[Only used when node self registration is enabled] Path to the unix "
     "domain socket the server will use to listen for other servers.",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("server-to-server-port", &server_to_server_port, "0", validate_optional_port,
     "[Only used when node self registration is enabled] TCP port on which the "
     "server listens for server-to-server connections. A value of zero means that "
     "the server listens for other servers on the base address.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("roles", &roles, "sequencer,storage",
     parse_roles,
     "[Only used when node self registration is enabled] Defines whether the "
     "node is sequencer node, storage node or both. The roles are "
     "comma-separated. e.g. 'sequencer,storage'",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("location", &location, "",
     parse_location,
     "[Only used when node self registration is enabled] The location of the "
     "node. Check the documentation of NodeLocation::fromDomainString to "
     "understand the format.",
     SERVER | REQUIRES_RESTART | CLI_ONLY,
     SettingsCategory::NodeRegistration)

    ("sequencer-weight", &sequencer_weight, "1",
     validate_positive<double>(),
     "[Only used when node self registration is enabled] define a proportional "
     "value for the number of sequencers to be placed on the machine",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("storage-capacity", &storage_capacity, "1",
     validate_positive<double>(),
     "[Only used when node self registration is enabled] defines a proportional "
     "value for the amount of data to be stored compared to other machines. "
     "When e.g. total disk size is used as weight for machines with variable "
     "disk sizes, the storage will be used proportionally. ",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("num-shards", &num_shards, "1",
     validate_positive<int>(),
     "[Only used when node self registration is enabled] defines how many "
     "storage shards this node will have. Sharding can be useful to distribute "
     "the IO load on multiple disks that are managed by the same daemon.",
     SERVER | REQUIRES_RESTART,
     SettingsCategory::NodeRegistration)

    ("wipe-storage-when-storage-state-none",
     &wipe_storage_when_storage_state_none,
     "false",
     nullptr,
     "Allow wiping the local RocksDB store when its storage state is none",
     SERVER | REQUIRES_RESTART,
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
