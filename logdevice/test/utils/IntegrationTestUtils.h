/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <folly/Optional.h>
#include <folly/Subprocess.h>
#include <folly/experimental/TestUtil.h>

#include "folly/io/async/EventBase.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/LogsConfigTypes.h"
#include "logdevice/include/types.h"
#include "logdevice/test/utils/MetaDataProvisioner.h"
#include "logdevice/test/utils/port_selection.h"

namespace facebook { namespace logdevice {

/**
 * @file Utilities for running LogDevice clusters in integration tests.
 *
 * In the common case, this suffices to spin up a LogDevice cluster in an
 * integration test:
 *
 *   // Start a LogDevice cluster with 5 nodes on localhost.
 *   auto cluster = logdevice::IntegrationTestUtils::ClusterFactory().create(5);
 *
 *   // Create a Client that can be used to write and read data.
 *   std::shared_ptr<logdevice::Client> client = cluster->createClient();
 *
 *   // Cluster shuts down when it goes out of scope.
 */

/*
 * Several environment variables alter how the utilities behave and can aid
 * debugging.
 *
 * LOGDEVICE_TEST_PAUSE_FOR_GDB=1 makes ClusterFactory pause the main process
 * right after starting the cluster, allowing the user to attach to server
 * processes with GDB.  For convenience, command lines to attach GDB are
 * printed to stderr.
 *
 * LOGDEVICE_TEST_LEAVE_DATA=1 makes Cluster not delete data from the
 * filesystem when shutting down.  For convenience, the path is logged at info
 * level.
 *
 * LOGDEVICE_TEST_BINARY controls which binary to run as the server.  If not
 * set, _bin/logdevice/server/logdeviced is used.
 *
 * LOGDEVICE_TEST_USE_TCP        use TCP ports instead of UNIX domain sockets
 *
 * LOGDEVICE_LOG_LEVEL           set the default log level used by tests
 *
 * LOGDEVICE_TEST_FORCE_SSL      forces all sockets to be SSL-enabled
 *
 * LOGDEVICE_TEST_NO_TIMEOUT     do not enforce timeout in tests
 *
 * LOGDEVICE_TEST_MESSAGE_ERROR_CHANCE   together defines chance and status
 * LOGDEVICE_TEST_MESSAGE_STATUS         parameters for message error injection
 *
 */

class Client;
class EpochStore;
class FileConfigSource;
class ShardedLocalLogStore;
class NodesConfigurationPublisher;

namespace test {
struct ServerInfo;
}
namespace thrift {
class AdminAPIAsyncClient;
}

namespace IntegrationTestUtils {

class Cluster;
class Node;

// scope in which command line parameters that applies to different
// types of nodes. Must be defined continuously.
enum class ParamScope : uint8_t { ALL = 0, SEQUENCER = 1, STORAGE_NODE = 2 };

using ParamValue = folly::Optional<std::string>;
using ParamMap = std::unordered_map<std::string, ParamValue>;
using ParamMaps = std::map<ParamScope, ParamMap>;

class ParamSpec {
 public:
  std::string key_;
  folly::Optional<std::string> value_;
  ParamScope scope_;

  /* implicit */ ParamSpec(std::string key, ParamScope scope = ParamScope::ALL)
      : key_(key), scope_(scope) {
    ld_check(!key_.empty());
  }

  ParamSpec(std::string key,
            std::string value,
            ParamScope scope = ParamScope::ALL)
      : ParamSpec(key, scope) {
    value_ = value;
  }
};

// used to specify the type of rockdb local logstore for storage
// nodes in the cluster
enum class RocksDBType : uint8_t { SINGLE, PARTITIONED };

/**
 * Configures a cluster and creates it.
 */
class ClusterFactory {
 public:
  /**
   * Creates a Cluster object, configured with logs 1 and 2.
   *
   * Unless setNodes() is called (in which case nnodes must equal nodes.size()),
   * Cluster will contain the specified number of nodes. If nnodes = 1, the one
   * node will act as both a sequencer and storage node.  Otherwise, there will
   * be one sequencer node and the rest will be storage nodes.
   *
   * By default, an initial epoch metadata will be provisioned for all logs
   * in the configuration, both in epoch store and in metadata storage nodes
   * as metadata log records. The nodeset in the metadata includes ALL nodes in
   * the cluster and replication factor is the value specified in LogConfig.
   * Regarding the epoch, for metadata log records in storage nodes, _epoch_ and
   * _effective_since_ are both EPOCH_MIN, while for metadata in the epoch
   * store, _epoch_ is set to be EPOCH_MIN+1 with _effective_since_ remains
   * EPOCH_MIN.
   *
   * A side effect of the provison is that sequencer nodes of the cluster
   * will be started on epoch 2 (EPOCH_MIN+1) and recovery will be performed
   * on sequencer nodes when the cluster starts.
   */
  std::unique_ptr<Cluster> create(int nnodes);

  /**
   * Creates a Cluster, specifying the full config to be used by the
   * cluster.  This allows for fine control of configuration.  Only the
   * addresses of nodes will be overwritten by the factory.
   */
  std::unique_ptr<Cluster> create(const Configuration& config);

  /**
   * Call the passed in function on this object. Typically used to
   * collect settings common to multiple test cases in a single
   * function, and apply them by adding a single function call in
   * each test case function. Example:
   *
   * static void commonOptions(IntegrationTestUtils::ClusterFactory& cluster) {
   *   cluster
   *     .setRocksDBType(IntegrationTestUtils::RocksDBType::PARTITIONED);
   * }
   *
   * ...
   *
   * auto cluster = IntegrationTestUtils::ClusterFactory()
   *  .apply(commonOptions)
   *  .setLogAttributes(log_attrs)
   *  .setEventLogAttributes(log_attrs)
   *  .setNumLogs(1)
   *  .create(nnodes);
   */
  template <typename F>
  ClusterFactory& apply(F fn) {
    fn(*this);
    return *this;
  }

  /**
   * Use tcp ports instead of unix domain sockets. This can be used for tests
   * that verify behaviors specific to TCP.
   */
  ClusterFactory& useTcp() {
    use_tcp_ = true;
    return *this;
  }

  /**
   * Sets the default log attributes to use for logs when using the simple
   * factory create(nnodes).  If this is never called, a default log config will
   * be used with reasonable replication parameters depending on nnodes.
   */
  ClusterFactory& setLogAttributes(logsconfig::LogAttributes log_attributes) {
    log_attributes_ = log_attributes;
    return *this;
  }

  /**
   * Set the attributes for the internal config log.
   */
  ClusterFactory& setConfigLogAttributes(logsconfig::LogAttributes attrs);

  /**
   * Set the attributes for the internal event log.
   */
  ClusterFactory& setEventLogAttributes(logsconfig::LogAttributes attrs);

  /**
   * Set the attributes for the internal event log delta.
   * NOTE: unlike setEventLogAttributes() above, does not set attributes
   * for the "event_log_snapshots" log.
   */
  ClusterFactory& setEventLogDeltaAttributes(logsconfig::LogAttributes attrs);

  /**
   * Set the attributes for the internal maintenance log
   */
  ClusterFactory& setMaintenanceLogAttributes(logsconfig::LogAttributes attrs);

  /**
   * Enables LogsConfigManager for clusters. Strongly recommend also calling
   * useHashBasedSequencerAssignment(), especially if creating log groups or
   * directories after startup, since that will:
   * a) enable lazy sequencer activation (since static activation won't work)
   * b) enable gossip, for failure detector, required for lazy activation
   * c) wait for all nodes to be marked as ALIVE via gossip
   */
  ClusterFactory& enableLogsConfigManager();

  /**
   * Sets the metadata log config to use for logs when using the simple factory
   * create(nnodes).  If this is never called, a default metadata log config
   * will be used with metadata log stored on all nodes and replication factor
   * set to be min(3, num_storage_nodes)
   */
  ClusterFactory&
  setMetaDataLogsConfig(Configuration::MetaDataLogsConfig meta_config) {
    meta_config_ = meta_config;
    return *this;
  }

  /**
   * Sets the number of logs in the config.  Logs will be numbered 1 through
   * `n'. Ignored when LogsConfigManager is enabled, use
   * setNumLogsConfigManagerLogs instead.
   */
  ClusterFactory& setNumLogs(int n) {
    num_logs_ = n;
    return *this;
  }

  /**
   * Sets that number of logs that needs to be created if LogsConfigManager is
   * enabled. It's created by client API calls after after bootstraping the
   * cluster. It's ignored when `defer_start_` is true.
   */
  ClusterFactory& setNumLogsConfigManagerLogs(int n) {
    num_logs_config_manager_logs_ = n;
    return *this;
  }

  /**
   * If called, create() will use specified node configs.
   */
  ClusterFactory& setNodes(Configuration::Nodes nodes) {
    node_configs_ = std::move(nodes);
    return *this;
  }

  /**
   * Set number of racks to spread the storage amongst. Ignored if the
   * node config is overridden with `setNodes()`. By default the number of racks
   * is 1. Nodes will be assigned to a rack in round robin fashion, ie if there
   * are 2 racks, nodes with nid % 2 == 0 will be in rack 1 and the others in
   * rack 2.
   */
  ClusterFactory& setNumRacks(int num_racks) {
    num_racks_ = num_racks;
    return *this;
  }

  /**
   * Set the number of shards to use on storage nodes.
   * Ignored if you configure nodes using setNodes().
   */
  ClusterFactory& setNumDBShards(int num_db_shards) {
    num_db_shards_ = num_db_shards;
    return *this;
  }

  /**
   * Sets the rocksdb type for storage nodes in the cluster
   */
  ClusterFactory& setRocksDBType(RocksDBType db_type) {
    rocksdb_type_ = db_type;
    return *this;
  }

  /**
   * Sets the node that is designated to run
   * maintenance manager
   */
  ClusterFactory& runMaintenanceManagerOn(node_index_t n) {
    maintenance_manager_node_ = n;
    return *this;
  }

  /**
   * If called, epoch metadata will be provisioned in epoch store and metadata
   * storage nodes on cluster startup.
   */
  ClusterFactory& doPreProvisionEpochMetaData() {
    provision_epoch_metadata_ = true;
    return *this;
  }

  /**
   * If called, nodes configuration store won't be provisioned.
   */
  ClusterFactory& doNotPreProvisionNodesConfigurationStore() {
    provision_nodes_configuration_store_ = false;
    return *this;
  }

  ClusterFactory& doNotSyncServerConfigToNodesConfiguration() {
    sync_server_config_to_nodes_configuration_ = false;
    return *this;
  }

  /**
   * By default, epoch store metadata is provisioned and metadata logs are
   * written by sequencers. If this method is called, sequencers will be
   * precluded from writing metadata. Note that this will have no effect if
   * ClusterFactory::setMetaDataLogsConfig() is called with a MetaDataLogsConfig
   * instance as an argument.
   */
  ClusterFactory& doNotLetSequencersProvisionEpochMetaData() {
    let_sequencers_provision_metadata_ = false;
    return *this;
  }

  /**
   * By default, the cluster factory generates a single config file that is
   * shared among all nodes. When this option is set, the factory will generate
   * one config file per node in their own directory. They will initially be
   * identical. but this allows testing with inconsistent configurations.
   * In paritcular this option is required to simulate netwrok paritioning (see
   * partition method below)
   * Note: expand/shrink/replace and maybe some other functionalities are not
   * compatible with this setting yet.
   *
   */
  ClusterFactory& oneConfigPerNode() {
    one_config_per_node_ = true;
    return *this;
  }

  /**
   * If metadata is to be provisioned by the test cluster, and it already
   * exists, the default behaviour is to fail provisioning with E::EXISTS.
   * Call this method to silently use existing metadata instead.
   */
  ClusterFactory& allowExistingMetaData() {
    allow_existing_metadata_ = true;
    return *this;
  }

  /**
   * Skips assigning SSL addresses to nodes.
   */
  ClusterFactory& noSSLAddress() {
    no_ssl_address_ = true;
    return *this;
  }

  enum class EventLogMode { NONE, DELTA_LOG_ONLY, SNAPSHOTTED };

  /**
   * @param mode one of:
   *             - EventLogMode::NONE: an event log is not provisioned
   *               in the cluster's config. TODO(#8466255): currently the event
   *               log is not mandatory in the config file. When we make it
   *               mandatory, all tests that use this option must be modified;
   *             - EventLogMode::DELTA_LOG_ONLY: no snapshot log is provisioned;
   *             - EventLogMode::SNAPSHOTTED: both the delta and snapshot logs
   *               are provisioned. If the --event-log-snapshotting setting is
   *               true, the content of the delta log will be periodically
   *               snapshotted onto this log.
   */
  ClusterFactory& eventLogMode(EventLogMode mode) {
    event_log_mode_ = mode;
    return *this;
  }

  /**
   * If called, epoch metadata will be provisioned using the specific nodeset
   * selector. Otherwise, SELECT_ALL will be used to selecto all nodes in the
   * cluster as the nodeset
   */
  ClusterFactory&
  setProvisionNodeSetSelector(std::shared_ptr<NodeSetSelector> selector) {
    provision_nodeset_selector_ = std::move(selector);
    return *this;
  }

  /**
   * Sets replication factor to use for internal and metadata logs if
   * set*LogsConfig() wasn't called.
   * If not called, the default is 3 for metadata log, 2 for internal logs.
   */
  ClusterFactory& setInternalLogsReplicationFactor(int r) {
    internal_logs_replication_factor_ = r;
    return *this;
  }

  /**
   * If called, create() will not immediately start all nodes after creating
   * a Cluster object. Instead, Cluster::start() should be called to run all
   * processes.
   */
  ClusterFactory& deferStart() {
    defer_start_ = true;
    return *this;
  }

  /**
   * Sets a command line parameter for logdeviced processes. The scope parameter
   * can be used to specify that the parameter is only for sequencer nodes or
   * storage nodes.
   */
  ClusterFactory& setParam(std::string key,
                           std::string value,
                           ParamScope scope = ParamScope::ALL) {
    return setParam(ParamSpec{key, value, scope});
  }

  /**
   * Same as setParam(key, value, scope) but for parameters without values
   */
  ClusterFactory& setParam(std::string key,
                           ParamScope scope = ParamScope::ALL) {
    return setParam(ParamSpec{key, scope});
  }

  /**
   * Same as setParam(key, value, scope) or setParam(key, scope) as appropriate.
   */
  ClusterFactory& setParam(ParamSpec spec) {
    cmd_param_[spec.scope_][spec.key_] = spec.value_;
    return *this;
  }

  /**
   * Sets the root directory for all the cluster's data.  If never called, a
   * temporary directory is created.
   */
  ClusterFactory& setRootPath(std::string path) {
    root_path_.assign(std::move(path));
    return *this;
  }

  /**
   * Use a gossip-based failure detector and spread logs across all sequencer
   * nodes (based on a hash function).
   */
  ClusterFactory&
  useHashBasedSequencerAssignment(uint32_t gossip_interval_ms = 100,
                                  std::string suspect_duration = "0ms") {
    setParam("--gossip-enabled", ParamScope::ALL);
    setParam("--gossip-interval",
             std::to_string(gossip_interval_ms) + "ms",
             ParamScope::ALL);
    setParam("--suspect-duration", suspect_duration, ParamScope::ALL);
    // lazy sequencer bringup
    setParam("--sequencers", "lazy", ParamScope::SEQUENCER);
    hash_based_sequencer_assignment_ = true;
    return *this;
  }

  ClusterFactory& enableSelfInitiatedRebuilding(std::string grace_period = "") {
    if (!grace_period.empty()) {
      setParam("--self-initiated-rebuilding-grace-period", grace_period);
    }
    return setParam("--enable-self-initiated-rebuilding", "true")
        .setParam("--disable-rebuilding", "false");
  }

  /**
   */
  ClusterFactory& enableMessageErrorInjection();

  /**
   */
  ClusterFactory& enableMessageErrorInjection(double chance, Status st) {
    setParam("--msg-error-injection-chance",
             std::to_string(chance),
             ParamScope::ALL);
    setParam("--msg-error-injection-status", error_name(st), ParamScope::ALL);

    if (chance != 0) {
      ld_info("Enabling message error injection with chance %.2f%% "
              "and status %s",
              chance,
              error_name(st));
    }
    return *this;
  }

  /**
   * Sets the path to the server binary (relative to the build root) to use if
   * a custom one is needed.
   */
  ClusterFactory& setServerBinary(std::string path) {
    server_binary_ = path;
    return *this;
  }

  /**
   * By default, the cluster will use a traffic shaping configuration which is
   * designed for coverage of the traffic shaping logic in tests, but limits
   * throughput.  This method allows traffic shaping to be turned off in cases
   * where performance matters.
   */
  ClusterFactory& useDefaultTrafficShapingConfig(bool use) {
    use_default_traffic_shaping_config_ = use;
    return *this;
  }

  /**
   * This will be passed to logdeviced as --loglevel option. More precisely,
   * --loglevel will be set to the first item on this list that's defined:
   *  1. "--loglevel" value set with setParam(),
   *  2. value passed to setLogLevel(),
   *  3. LOGDEVICE_LOG_LEVEL environment variable,
   *  4. "info".
   */
  ClusterFactory& setLogLevel(dbg::Level log_level) {
    default_log_level_ = log_level;
    return *this;
  }

  /**
   * Write logs config to a file separately and include this file from the main
   * config file
   */
  ClusterFactory& writeLogsConfigFileSeparately() {
    write_logs_config_file_separately_ = true;
    return *this;
  }

  /**
   * Value of the cluster_name property in config.
   * Affects how stats are exported.
   */
  ClusterFactory& setClusterName(std::string name) {
    cluster_name_ = name;
    return *this;
  }

  ClusterFactory& setLogGroupName(const std::string& name) {
    log_group_name_ = name;
    return *this;
  }

  /**
   * Generates a default log attribute (replication, extras) based on the
   * cluster size.  This is used internally if setLogAttributes() is not called.
   * Exposed so that the logic can be reused.
   */
  static logsconfig::LogAttributes
  createDefaultLogAttributes(int nstorage_nodes);

 private:
  folly::Optional<logsconfig::LogAttributes> log_attributes_;
  folly::Optional<Configuration::Nodes> node_configs_;
  folly::Optional<Configuration::MetaDataLogsConfig> meta_config_;
  bool enable_logsconfig_manager_ = false;
  bool enable_ncm_ = true;
  bool one_config_per_node_{false};

  configuration::InternalLogs internal_logs_;

  ParamMaps cmd_param_;

  // If set to true, allocate tcp ports to be used by the tests for the nodes'
  // protocol and command ports instead of unix domain sockets.
  bool use_tcp_ = false;

  // How many times to try the entire process of starting up the cluster (pick
  // ports, start servers, wait for them to start).  Only applies when
  // `use_tcp_' is true as we don't expect flaky startup with Unix domain
  // sockets.
  int outer_tries_ = 5;
  int outerTries() const {
    return use_tcp_ ? outer_tries_ : 1;
  }

  // Provision the inital epoch metadata in epoch store and storage nodes
  // that store metadata
  bool provision_epoch_metadata_ = false;

  // Provision the inital nodes configuration store
  bool provision_nodes_configuration_store_ = true;

  // Controls whether the cluster should also update the NodesConfiguration
  // whenver the ServerConfig change. This is there only during the migration
  // period.
  bool sync_server_config_to_nodes_configuration_ = true;

  // Whether to let sequencers provision metadata
  bool let_sequencers_provision_metadata_ = true;

  // Allow pre-existing metadata when provisioning
  bool allow_existing_metadata_ = false;

  // Don't set SSL addresses on nodes
  bool no_ssl_address_ = false;

  // @see useDefaultTrafficShapingConfig()
  bool use_default_traffic_shaping_config_{true};

  // Defines how we should provision the event log.
  EventLogMode event_log_mode_{EventLogMode::DELTA_LOG_ONLY};

  // nodeset selector used for provisioning epoch metadata
  std::shared_ptr<NodeSetSelector> provision_nodeset_selector_;

  // Don't start all nodes when Cluster is created
  bool defer_start_ = false;

  // How many logs in the config
  int num_logs_ = 2;

  int num_logs_config_manager_logs_ = 0;

  // Number of shards for each storage node
  int num_db_shards_ = 2;

  // Number of racks to spread the nodes amongst.
  int num_racks_ = 1;

  // See setInternalLogsReplicationFactor().
  int internal_logs_replication_factor_ = -1;

  // If set to true, logs are assumed to be spread across all sequencer nodes.
  // Otherwise, all appends are sent to the first node in the cluster.
  bool hash_based_sequencer_assignment_{false};

  // The node designated to run a instance of MaintenanceManager
  node_index_t maintenance_manager_node_ = -1;

  // Type of rocksdb local log store
  RocksDBType rocksdb_type_ = RocksDBType::PARTITIONED;

  // Root path for all data if setRootPath() was called
  folly::Optional<std::string> root_path_;

  // Server binary if setServerBinary() was called
  folly::Optional<std::string> server_binary_;

  std::string cluster_name_ = "integration_test";

  std::string log_group_name_ = "/ns/test_logs";

  // See setLogLevel().
  dbg::Level default_log_level_ =
      getLogLevelFromEnv().value_or(dbg::Level::INFO);

  // See writeLogsConfigFileSeparately()
  bool write_logs_config_file_separately_{false};

  // Helper method, one attempt in create(), repeated up to outer_tries_ times
  std::unique_ptr<Cluster> createOneTry(const Configuration& config);

  static logsconfig::LogAttributes createLogAttributesStub(int nstorage_nodes);

  // Figures out the full path to the server binary, considering in order of
  // precedence:
  //
  // - the environment variable LOGDEVICED_TEST_BINARY,
  // - setServerBinary() override
  // - a default path
  std::string actualServerBinary() const;

  // Set the attributes of an internal log.
  void setInternalLogAttributes(const std::string& name,
                                logsconfig::LogAttributes attrs);

  /**
   * Uses either the provided log_config_ or creates a new default one to
   * create a new logs config manager based log group. It requires that the
   * cluster is up and running.
   */
  std::unique_ptr<client::LogGroup>
  createLogsConfigManagerLogs(std::unique_ptr<Cluster>& cluster);
};

struct SockaddrPair {
  Sockaddr protocol_addr_;
  Sockaddr command_addr_;
  Sockaddr gossip_addr_;
  Sockaddr admin_addr_;
  // If one of protocol_addr_ or command_addr_ contains a TCP port, we own a
  // detail::PortOwner object for it that keeps a socket bound to it.
  detail::PortOwnerPtrTuple tcp_ports_owned_;

  static SockaddrPair fromTcpPortPair(detail::PortOwnerPtrTuple tuple) {
    SockaddrPair addrs;
    addrs.protocol_addr_ =
        Sockaddr(get_localhost_address_str(), std::get<0>(tuple)->port);
    addrs.command_addr_ =
        Sockaddr(get_localhost_address_str(), std::get<1>(tuple)->port);
    addrs.admin_addr_ =
        Sockaddr(get_localhost_address_str(), std::get<2>(tuple)->port);
    addrs.tcp_ports_owned_ = std::move(tuple);
    return addrs;
  }

  static void buildGossipTcpSocket(SockaddrPair& addr, int port) {
    addr.gossip_addr_ = Sockaddr(get_localhost_address_str(), port);
  }

  static SockaddrPair buildGossipSocket(const std::string& path) {
    SockaddrPair addrs;
    addrs.gossip_addr_ = Sockaddr(path + "/" + "socket_gossip");
    return addrs;
  }

  static SockaddrPair buildUnixSocketPair(const std::string& path, bool ssl) {
    SockaddrPair addrs;
    std::string ssl_prefix = (ssl ? "ssl_" : "");
    addrs.protocol_addr_ =
        Sockaddr(folly::format("{}/{}socket_main", path, ssl_prefix).str());
    // Gossip currently never uses SSL
    addrs.gossip_addr_ =
        Sockaddr(folly::format("{}/socket_gossip", path).str());
    addrs.command_addr_ =
        Sockaddr(folly::format("{}/{}socket_command", path, ssl_prefix).str());
    addrs.admin_addr_ =
        Sockaddr(folly::format("{}/{}socket_admin", path, ssl_prefix).str());
    return addrs;
  }
};

/**
 * RAII-style container for a LogDevice cluster running on localhost.
 */
class Cluster {
 public:
  using Nodes = std::map<node_index_t, std::unique_ptr<Node>>;

  ~Cluster();

  /**
   * Used in conjunction with ClusterFactory::deferStart() to run a process for
   * each node in the cluster. Waits for all nodes to start.
   *
   * @param indices   if non-empty, only a specified subset of nodes will be
   *                  started
   *
   * @return 0 on success, -1 if any of the nodes fails to start
   */
  int start(std::vector<node_index_t> indices = {});

  /**
   * Kill all running nodes in the cluster.
   */
  void stop();

  /**
   * Expand the cluster by adding nodes with the given indices.
   * @return 0 on success, -1 on error.
   */
  int expand(std::vector<node_index_t> new_indices, bool start = true);

  /**
   * Expand the cluster by adding `nnodes` with consecutive indices after the
   * highest existing one.
   * @return 0 on success, -1 on error.
   */
  int expand(int nnodes, bool start = true);

  /**
   * Shrink the cluster by removing the given nodes.
   * @return 0 on success, -1 on error.
   */
  int shrink(std::vector<node_index_t> indices);

  /**
   * Shrink the cluster by removing `nnodes` last nodes.
   * @return 0 on success, -1 on error.
   */
  int shrink(int nnodes);

  std::shared_ptr<UpdateableConfig> getConfig() {
    return config_;
  }

  std::string getConfigPath() const {
    return config_path_;
  }

  std::string getNCSPath() const {
    return ncs_path_;
  }

  /**
   * Like ClientFactory::create(), but:
   *  - tweaks some client settings to be more appropriate for tests,
   *  - the created client taps into the UpdateableConfig instance owned by
   *    this Cluster object. This speeds up client creation.
   * Creating a client can take a few seconds, so reuse them when possible.
   */
  std::shared_ptr<Client>
  createClient(std::chrono::milliseconds timeout = getDefaultTestTimeout(),
               std::unique_ptr<ClientSettings> settings =
                   std::unique_ptr<ClientSettings>(),
               std::string credentials = "");

  /**
   * This creates a client by calling ClientFactory::create() that does not
   * share the loaded config_. This function should be removed and instead we
   * should update createClient to do the same. t18313631 tracks this and
   * explains the reasons behind this.
   */
  std::shared_ptr<Client> createIndependentClient(
      std::chrono::milliseconds timeout = getDefaultTestTimeout(),
      std::unique_ptr<ClientSettings> settings =
          std::unique_ptr<ClientSettings>()) const;

  const Nodes& getNodes() const {
    return nodes_;
  }

  Node& getNode(node_index_t index) {
    ld_assert(nodes_.count(index));
    ld_check(nodes_[index] != nullptr);
    return *nodes_.at(index);
  }

  Node& getSequencerNode() {
    // For now, the first node is always the sequencer
    return getNode(0);
  }

  // When using hash-based sequencer assignment, the above is not sufficient.
  // Hash-based sequencer assignment is necessary to have failover.
  // Returns -1 if there is no sequencer for the log or it is unavailable.
  int getHashAssignedSequencerNodeId(logid_t log_id, Client* client);

  // Call function for every node. Function signature is F(Node&).
  // By default processes each node in its own thread.
  // Set use_threads = false to do everything in the calling thread.
  template <typename F>
  void applyToNodes(F func, bool use_threads = true) {
    NodeSetIndices nodes;
    for (auto& node : nodes_) {
      nodes.push_back(node.first);
    }
    applyToNodes(nodes, func, use_threads);
  }

  template <typename F>
  void applyToNodes(const NodeSetIndices& nodeset,
                    F func,
                    bool use_threads = true) {
    if (use_threads) {
      std::vector<std::thread> ts;
      for (node_index_t nidx : nodeset) {
        ts.emplace_back([this, func, nidx] { func(getNode(nidx)); });
      }
      for (auto& t : ts) {
        t.join();
      }
    } else {
      for (node_index_t nidx : nodeset) {
        func(getNode(nidx));
      }
    }
  }

  /**
   * Returns an EpochStore object representing the store that a sequencer node
   * will use. Intended to be used with ClusterFactory::deferStart() to set
   * initial epochs for logs before starting nodes.
   */
  std::unique_ptr<EpochStore> createEpochStore();

  /**
   * Updates epoch store to set the next epoch for log_id
   */
  void setStartingEpoch(logid_t log_id,
                        epoch_t epoch,
                        epoch_t last_expected_epoch = EPOCH_INVALID);

  /**
   * Provision the initial epoch metadata on metadata storage nodes,
   * must be called when the storage nodes are not started
   *
   * @param selector                  nodeset selector for provisioning, if not
   *                                  given, SELECT_ALL is used
   * @param allow_existing_metadata   whether provisioning will succeed if a log
   *                                  is already provisioned. If this is false,
   *                                  it will fail with E::EXISTS.
   * @return          0 for success, -1 for failure
   */
  int provisionEpochMetaData(
      std::shared_ptr<NodeSetSelector> selector = nullptr,
      bool allow_existing_metadata = false);

  /**
   * Converts the server config into a nodes configuration and writes it to
   * disk via a FileBasedNodesConfigurationStore.
   * @param server_config                  the server config to convert
   * @return          0 for success, -1 for failure
   */
  int updateNodesConfigurationFromServerConfig(
      const ServerConfig* server_config);

  /**
   * Replaces the node at the specified index.  Kills the current process if
   * still running, deletes the node's data, then starts up a new one and
   * updates the cluster config.
   * @return 0 on success, -1 if node fails to start or there are no free ports
   */
  int replace(node_index_t index, bool defer_start = false);

  /**
   * Update the config to bump the generation of node at position `index`.
   * Also bump the node replacement counter.
   */
  int bumpGeneration(node_index_t index);

  /**
   * Update node's attributes in config
   */
  int updateNodeAttributes(
      node_index_t index,
      configuration::StorageState storage_state,
      int sequencer_weight,
      folly::Optional<bool> enable_sequencing = folly::none);

  /**
   * Waits until all live nodes have a view of the config same as getConfig().
   */
  void waitForConfigUpdate();

  /**
   * Wait for all sequencer nodes in the cluster to finish log recovery.
   * Caller needs to ensure recovery should happen on sequencer nodes.
   * @return 0 if recovery is completed, -1 if the call timed out.
   */
  int waitForRecovery(std::chrono::steady_clock::time_point deadline =
                          std::chrono::steady_clock::time_point::max());

  // Waits until all nodes are available through gossip (ALIVE)
  int waitUntilAllAvailable(std::chrono::steady_clock::time_point deadline =
                                std::chrono::steady_clock::time_point::max());

  /**
   * Wait for all sequencer nodes in the cluster to write metadata log records
   * for all logs. This shouldn't block if sequencers_write_metadata_logs is
   * `false` in the metadata logs config.
   * @return 0 if all metadata logs were written, -1 if the call timed out.
   */

  int waitForMetaDataLogWrites(
      std::chrono::steady_clock::time_point deadline =
          std::chrono::steady_clock::time_point::max());

  /**
   * Wait for all nodes in the cluster except the ones specified in the skip
   * list to see the specified node in a DEAD/ALIVE state (depending on what is
   * submitted as the `alive` arg
   */

  int waitUntilGossip(bool alive, /* set to false for waiting for dead */
                      uint64_t targetNode,
                      std::set<uint64_t> nodesToSkip = {},
                      std::chrono::steady_clock::time_point deadline =
                          std::chrono::steady_clock::time_point::max());

  /**
   * Same as ClusterFactory::setParam(). Only affects future logdeviced
   * instances, like the ones created by replace().
   */
  void setParam(std::string key, ParamScope scope = ParamScope::ALL) {
    ld_check(!key.empty());
    cmd_param_[scope][key] = ParamValue();
  }
  void setParam(std::string key,
                std::string value,
                ParamScope scope = ParamScope::ALL) {
    ld_check(!key.empty());
    cmd_param_[scope][key] = value;
  }

  /**
   * Undoes what setParam() did.
   */
  void unsetParam(std::string key, ParamScope scope = ParamScope::ALL) {
    ld_check(!key.empty());
    cmd_param_[scope].erase(key);
  }

  /**
   * Check that all the data in the cluster is correctly replicated.
   *
   * @return 0 if all the data is correctly replicated, -1 otherwise.
   */
  using argv_t = std::vector<std::string>;
  int checkConsistency(argv_t additional_args = argv_t());

  /**
   * Convenience function that creates a MetaDataProvisioner object for
   * provisioning epoch metadata for logs on the cluster.
   * User of the provisioner object must ensure that the object will not
   * outlive the Cluster object.
   */
  std::unique_ptr<MetaDataProvisioner> createMetaDataProvisioner();

  /**
   * Read the event log of the cluster and build a ShardAuthoritativeStatusMap.
   * @param map Filled with the state read from the event log.
   * @return 0 on success, -1 on failure and err is set to:
   *   - E::NOTFOUND if the cluster has no event log;
   *   - Any error that can be reported by Client::getTailLSNSync() if this
   *     function could not retrieve the tail LSN of the event log;
   *   - Any error that can be reported by Reader::startReading() if this
   *     function cannot start reading the event log.
   */
  int getShardAuthoritativeStatusMap(ShardAuthoritativeStatusMap& map);

  /**
   * Wait until all nodes in @param nodes have read the logs config delta log up
   * to @param sync_lsn.
   */
  int waitUntilLogsConfigSynced(
      lsn_t sync_lsn,
      const std::vector<node_index_t>& nodes,
      std::chrono::steady_clock::time_point deadline =
          std::chrono::steady_clock::time_point::max());

  /**
   * Wait until all nodes in @param nodes have read the event log up to
   * @param sync_lsn.
   */
  int waitUntilEventLogSynced(lsn_t sync_lsn,
                              const std::vector<node_index_t>& nodes,
                              std::chrono::steady_clock::time_point deadline =
                                  std::chrono::steady_clock::time_point::max());

  /**
   * Partitions cluster by overwriting individual node's config with invalid
   * address for each node belonging to a different partition. Note that, upon
   * receiving the config update, each node is going to close exsiting
   * connections to nodes outside of their partition.
   */
  void partition(std::vector<std::set<int>> partitions);

  /**
   * Waits until all nodes satisfy the given predicate
   */
  void waitUntilAll(const char* desc, std::function<bool(Node&)> pred);

  /**
   * Gracefully shut down the given nodes. Faster than calling shutdown() on
   * them one by one.
   * @return 0 if all processes returned zero exit status, -1 otherwise.
   */
  int shutdownNodes(const std::vector<node_index_t>& nodes);

  /**
   * Overwrites config file. If wait_for_update is true, waits for config_ to
   * pick up the update.
   * Note that if the update is going to be rejected, e.g. because the version
   * is smaller than current, wait_for_update would make this method wait
   * forever.
   *
   * Use waitForConfigUpdate() to wait for nodes to pick up the update.
   */
  int writeConfig(const ServerConfig* server_cfg,
                  const LogsConfig* logs_cfg,
                  bool wait_for_update = true);
  int writeConfig(const Configuration& cfg, bool wait_for_update = true);

  // Convenience wrappers
  int writeServerConfig(const ServerConfig* server_cfg) {
    return writeConfig(server_cfg, getConfig()->getLogsConfig().get());
  }
  int writeLogsConfig(const LogsConfig* logs_cfg) {
    return writeConfig(getConfig()->getServerConfig().get(), logs_cfg);
  }

  // see node_replacement_counters_ below
  node_gen_t getNodeReplacementCounter(node_index_t node) const {
    return node_replacement_counters_.count(node) > 0
        ? node_replacement_counters_.at(node)
        : 1;
  }
  void setNodeReplacementCounter(node_index_t node, node_gen_t rc) {
    node_replacement_counters_[node] = rc;
  }
  void bumpNodeReplacementCounter(node_index_t node) {
    ++node_replacement_counters_[node];
  }
  void setNodeReplacementCounters(std::map<node_index_t, node_gen_t> counters) {
    node_replacement_counters_ = std::move(counters);
  }

  // require @param node must exist in the cluster
  bool hasStorageRole(node_index_t node) const;

 private:
  // Private constructor.  Factory (friend class) is only caller.
  Cluster(std::string root_path,
          std::unique_ptr<folly::test::TemporaryDirectory> root_pin,
          std::string config_path,
          std::string epoch_store_path,
          std::string ncs_path,
          std::string server_binary,
          std::string cluster_name,
          bool enable_logsconfig_manager,
          bool one_config_per_node,
          dbg::Level default_log_level,
          bool write_logs_config_file_separately,
          bool sync_server_config_to_nodes_configuration);

  // Directory where to store the data for a node (logs, db, sockets).
  static std::string getNodeDataPath(const std::string& root,
                                     node_index_t index,
                                     int replacement_counter) {
    return root + "/N" + std::to_string(index) + ':' +
        std::to_string(replacement_counter);
  }

  std::string getNodeDataPath(const std::string& root,
                              node_index_t index) const {
    return getNodeDataPath(root, index, getNodeReplacementCounter(index));
  }

  // Creates a Node instance for the specified config entry and starts the
  // process.  Does not wait for process to start; call
  // node->waitUntilStarted() for that.
  std::unique_ptr<Node> createNode(node_index_t index,
                                   SockaddrPair addrs,
                                   SockaddrPair ssl_addrs) const;
  // Helper for createNode().  Figures out the initial command line args for the
  // specified node
  ParamMap commandArgsForNode(node_index_t index, const Node& node) const;

  // Helper for createClient() and createIndependentClient() to populate client
  // settings.
  void populateClientSettings(std::unique_ptr<ClientSettings>& settings) const;

  // We keep track whether the cluster was created using tcp ports or unix
  // domain sockets so that we can use the same method for new nodes created by
  // the expand() method.
  bool use_tcp_{false};

  // How many times to try starting a server
  int outer_tries_ = 2;

  // How long we wait for servers to start.  Only applies when
  // `use_tcp_' is true as we don't expect flaky startup with Unix domain
  // sockets.
  std::chrono::seconds start_timeout_{30};

  std::string root_path_;
  // If root_path_ is a temporary directory, this owns it
  std::unique_ptr<folly::test::TemporaryDirectory> root_pin_;
  std::string config_path_;
  std::string epoch_store_path_;
  // path for the file-based nodes configuration store
  std::string ncs_path_;
  std::string server_binary_;
  std::string cluster_name_;
  bool enable_logsconfig_manager_ = false;
  bool enable_ncm_ = true;
  bool one_config_per_node_ = false;
  std::shared_ptr<UpdateableConfig> config_;
  std::unique_ptr<NodesConfigurationPublisher> nodes_configuration_publisher_;
  FileConfigSource* config_source_;
  std::unique_ptr<ClientSettings> client_settings_;
  // ordered map for convenience
  Nodes nodes_;

  // keep track of node replacement events. for nodes with storage role, the
  // counter should be in sync with the `generation' in its config. For nodes
  // without storage role, counter is only used for tracking/directory keeping
  // purpose but not reflected in the config
  std::map<node_index_t, node_gen_t> node_replacement_counters_;

  // command line parameters, set by the Factory
  ParamMaps cmd_param_;

  int num_db_shards_ = 4;

  // type of rocksdb local log store
  RocksDBType rocksdb_type_ = RocksDBType::PARTITIONED;

  // The node designated to run a instance of MaintenanceManager
  node_index_t maintenance_manager_node_ = -1;

  // See ClusterFactory::hash_based_sequencer_assignment_
  bool hash_based_sequencer_assignment_{false};

  dbg::Level default_log_level_ = dbg::Level::INFO;

  bool write_logs_config_file_separately_{false};

  // Controls whether the cluster should also update the NodesConfiguration
  // whenver the ServerConfig change. This is there only during the migration
  // period.
  bool sync_server_config_to_nodes_configuration_{false};

  bool no_ssl_address_{false};

  // keep handles around until the cluster is destroyed.
  std::vector<UpdateableServerConfig::HookHandle> server_config_hook_handles_;

  friend class ClusterFactory;
};

/**
 * RAII-style container for a LogDevice server that is part of a Cluster.
 */
class Node {
 public:
  std::unique_ptr<folly::Subprocess> logdeviced_;
  std::string data_path_;
  std::string config_path_;
  std::string server_binary_;
  node_index_t node_index_;
  SockaddrPair addrs_; // Pair of Sockaddr to use for the protocol socket
  // and command socket.
  SockaddrPair ssl_addrs_; // Pair of Sockaddr to use for the protocol socket
  // and command socket in SSL.
  int num_db_shards_ = 4; // how many shards storage nodes will use
  // Random ID generated by constructor.  Passed on the command line to the
  // server.  waitUntilStarted() looks for this to verify that we are talking
  // to the right process.
  std::string server_id_;
  // Stopped until start() is called, as well as between suspend() and resume()
  bool stopped_ = true;
  // type of rocksdb local log store
  RocksDBType rocksdb_type_ = RocksDBType::PARTITIONED;
  // override cluster params for this particular node
  ParamMap cmd_args_;

  Node();
  ~Node() {
    kill();
  }

  /**
   * Creates a local log store instance for this node. Can be used with
   * ClusterFactory::deferStart() to prepopulate the store before logdeviced
   * is started, or to inspect the store after the node is stopped.
   */
  std::unique_ptr<ShardedLocalLogStore> createLocalLogStore();

  // Corrupts rocksdb DBs for given shards. rocksdb::DB::Open() will fail with
  // "Corruption" status.
  // If you've already called createLocalLogStore(), you can pass the result
  // here as `store` parameter, as an optimization to avoid opening DB again;
  // this method will close it.
  void
  corruptShards(std::vector<uint32_t> shards,
                std::unique_ptr<ShardedLocalLogStore> sharded_store = nullptr);

  void updateSetting(std::string name, std::string value);
  void unsetSetting(std::string name);

  std::string getDatabasePath() const {
    return data_path_ + "/db";
  }

  std::string getLogPath() const {
    return data_path_ + "/log";
  }

  Sockaddr getCommandSockAddr() const {
    return addrs_.command_addr_;
  }

  void signal(int sig) {
    logdeviced_->sendSignal(sig);
  }

  /**
   * @return true if logdeviced is running.
   */
  bool isRunning() const;

  void kill();

  /**
   * Wipe the content of a shard on this node.
   */
  void wipeShard(uint32_t shard);

  /**
   * Pauses logdeviced by sending SIGSTOP.  Waits for the process to stop
   * accepting connections.
   */
  void suspend();

  /**
   * Resume logdeviced by sending SIGCONT.  Waits for the process to start
   * accepting connections again.
   */
  void resume();

  /**
   * Starts logdeviced if not started already (without waiting for it to become
   * ready).
   */
  void start();

  /**
   * Restart server process and wait for it to be available if requested
   */
  void restart(bool graceful = true, bool wait_until_available = true);

  /**
   * Performs a graceful shutdown of logdeviced by issuing a "stop" admin
   * command.
   *
   * @return logdeviced exit code.
   */
  int shutdown();

  // Creates a thrift client for admin server running on this node.
  std::unique_ptr<thrift::AdminAPIAsyncClient> createAdminClient();

  /**
   * Waits until the admin API is able to answer requests that need the event
   * log. This also ensures that we are in the fb303 ALIVE state before
   * returning.
   *
   * Note: this requires that the server is started with
   * --disable-rebuilding=false
   */
  int waitUntilNodeStateReady();
  /**
   * Waits until the ClusterMaintenanceStateMachine is fully loaded on that
   * machine.
   */
  int waitUntilMaintenanceRSMReady();

  /**
   * Waits for the server to start accepting connections.
   * @return 0 if started, -1 if the call timed out.
   */
  int waitUntilStarted(std::chrono::steady_clock::time_point deadline =
                           std::chrono::steady_clock::time_point::max());

  /**
   * Waits for the server using a gossip-based failure detector to mark itself
   * as available (i.e. ready to process appends).
   * @return 0 if available, -1 if the call timed out.
   */
  int waitUntilAvailable(std::chrono::steady_clock::time_point deadline =
                             std::chrono::steady_clock::time_point::max());

  void waitUntilKnownDead(node_index_t other_node_index);

  /**
   * Waits for the server using a gossip-based failure detector to mark another
   * node as alive (if `alive` is set to `true`) or dead.
   *
   * @return 0 if succeeded, -1 if timed out while waiting
   */
  int waitUntilKnownGossipState(
      node_index_t other_node_index,
      bool alive,
      std::chrono::steady_clock::time_point deadline =
          std::chrono::steady_clock::time_point::max());

  /**
   * Waits for the node to activate a sequencer for this log and finish
   * recovery.
   */
  int waitForRecovery(logid_t log,
                      std::chrono::steady_clock::time_point deadline =
                          std::chrono::steady_clock::time_point::max());

  /**
   * Waits for the node to advance its LCE of @param log to be at least
   * @param epoch.
   */
  int waitForPurge(logid_t log,
                   epoch_t epoch,
                   std::chrono::steady_clock::time_point deadline =
                       std::chrono::steady_clock::time_point::max());

  /**
   * Wait until the node have read logsconfig delta log up to @param sync_lsn
   * and propagated it to all workers.
   */
  int waitUntilLogsConfigSynced(
      lsn_t sync_lsn,
      std::chrono::steady_clock::time_point deadline =
          std::chrono::steady_clock::time_point::max());

  /**
   * Wait until the node have read the event log up to @param sync_lsn and
   * propagated it to all workers.
   * Note that the propagation is delayed by --event-log-grace-period, so if
   * you're using this method you probably want to decrease
   * --event-log-grace-period.
   */
  int waitUntilEventLogSynced(lsn_t sync_lsn,
                              std::chrono::steady_clock::time_point deadline =
                                  std::chrono::steady_clock::time_point::max());

  /**
   * Wait until all shards of this node are fully authoritative in event log.
   * Returns the lsn of the last update.
   * Does NOT wait for this information to propagate to the node itself;
   * use waitUntilEventLogSynced() for that.
   */
  lsn_t waitUntilAllShardsFullyAuthoritative(std::shared_ptr<Client> client);
  /**
   * Wait until all shards of this node are authoritative empty.
   * Returns the lsn of the last update.
   * Does NOT wait for this information to propagate to the node itself;
   * use waitUntilEventLogSynced() for that.
   */
  lsn_t waitUntilAllShardsAuthoritativeEmpty(std::shared_ptr<Client> client);

  /**
   * Sends admin command `command' to command port and returns the result.
   * Connect through SSL if requested.
   */
  std::string sendCommand(const std::string& command, bool ssl = false) const;

  /**
   * Returns the admin API address for this node
   */
  folly::SocketAddress getAdminAddress() const;

  /**
   * Sends the provided admin command via the address of the interface with the
   * given name, and returns the result.
   */
  std::string sendIfaceCommand(const std::string& command,
                               const std::string ifname) const;

  /**
   * Finds and returns the address of the given interface on the node.
   */
  std::string getIfaceAddr(const std::string ifname) const;

  /**
   * Connects to the admin ports and returns the running server information
   */
  folly::Optional<test::ServerInfo> getServerInfo() const;

  /**
   * Waits for the logdeviced process to exit.
   * @return logdeviced return code.
   */
  int waitUntilExited();

  /**
   * Issues a STATS command to the node's command port and collects all stats
   * into a map.
   *
   * May return an empty map if the node is not up or not ready to accept
   * admin commands.
   */
  std::map<std::string, int64_t> stats() const;

  /**
   * Issues a COMPACT command to the node's command port and force a compaction
   * on the rocksdb locallogstore shard for the given logid. Pass in
   * LOGID_INVALID (default) as logid will let the node perform compaction
   * on all rocksdb shards.
   */
  int compact(logid_t logid = LOGID_INVALID) const;

  /**
   * Issues a LOGSTORAGESTATE command to the node's command port and collects
   * the result into a map.
   *
   * May return an empty map if the node is not up or not ready to accept
   * admin commands.
   */
  std::map<std::string, std::string> logState(logid_t log_id) const;

  /**
   * Issues a UP DOWN command to activate a sequencer for a given log_id on a
   * particular node
   * Returns a rsponse as a string
   */
  std::string upDown(const logid_t log_id) const;

  /**
   * Issues an INFO SEQUENCER command to the node's command port and collects
   * the results in a map.
   * Returns an empty map if there is no sequencer for the log.
   */
  std::map<std::string, std::string> sequencerInfo(logid_t log_id) const;

  /**
   * Issues a GOSSIP BLACKLIST command, and EXPECT_EQs that it succeeds.
   */
  void gossipBlacklist(node_index_t node_id) const;

  /**
   * Issues a GOSSIP WHITELIST command, and EXPECT_EQs that it succeeds.
   */
  void gossipWhitelist(node_index_t node_id) const;

  /**
   * Issues an INJECT SHARD_FAULT command
   * @returns false if in non-debug mode, as the command is only supported for
   * DEBUG builds, true otherwise.
   */
  bool injectShardFault(std::string shard,
                        std::string data_type,
                        std::string io_type,
                        std::string code,
                        bool single_shot = false,
                        folly::Optional<double> chance = folly::none,
                        folly::Optional<uint32_t> latency_ms = folly::none);
  /**
   * Issues a NEWCONNECTIONS command, and EXPECT_EQs that it succeeds.
   */
  void newConnections(bool accept) const;

  /**
   * Issues a STARTRECOVERY command, and EXPECT_EQs that it succeeds.
   */
  void startRecovery(logid_t logid) const;

  /**
   * Issues an INFO LOGSCONFIG_RSM command to the node's command port and
   * collects the results in a map. Returns an empty map if the node is not
   * reading the event log.
   */
  std::map<std::string, std::string> logsConfigInfo() const;

  /**
   * Issues an INFO EVENT_LOG command to the node's command port and collects
   * the results in a map.
   * Returns an empty map if the node is not reading the event log.
   */
  std::map<std::string, std::string> eventLogInfo() const;

  /**
   * Issues an INFO SOCKET command to the node's command port and collects
   * the results in a vector of maps.
   */
  std::vector<std::map<std::string, std::string>> socketInfo() const;

  /**
   * Issues an INFO PARTITIONS command to the node's command port and collects
   * the results in a vector of maps.
   */
  std::vector<std::map<std::string, std::string>> partitionsInfo() const;

  /**
   * Issues an INFO GOSSIP command to the node's command port to collect info
   * about the availability of other nodes. Results are stored in the map, with
   * keys corresponding to nodes, and values being either "ALIVE" or "DEAD".
   * Cluster has to be started with the --gossip-enable option.
   */
  std::map<std::string, std::string> gossipInfo() const;

  /**
   * Issues an INFO GOSSIP command to collect information about whether the node
   * is in starting state and display it.
   */
  std::map<std::string, bool> gossipStarting() const;

  /*
   * Sends "info gossip" to command port via nc.
   * Returns a map with one of the following state strings as value
   * ""        : If node is not in config
   * "DEAD"    : If node is DEAD
   * "SUSPECT" : If node is SUSPECT
   * "ALIVE"   : If node is ALIVE
   */
  std::map<std::string, std::string> gossipState() const;

  /*
   * Sends "info gossip" to command port via nc.
   *
   * Returns a map where the value is a pair of status (like gossipInfo() or
   * gossipState()) and the count of the number of gossip time intervals where
   * we haven't recevied a message.
   */
  std::map<std::string, std::pair<std::string, uint64_t>> gossipCount() const;

  /**
   * Sends "info gossip" to command port via nc.
   *
   * Returns a map where the key is the node name and the value is true if the
   * node is boycotted, false otherwise
   */
  std::map<std::string, bool> gossipBoycottState() const;

  void resetBoycott(node_index_t node_index) const;

  /**
   * Issues an INFO GOSSIP command to the node's command port to collect info
   * about the isolation status of local domains in all different node location
   * scopes. Results are stored in the map, with a special key-value pair of
   * "enabled" : "true"/"false" indicating if domain isolation dection is
   * enabled, and key-value pairs of "<scope name>" : "ISOLATED"/"NOT_ISOLATED".
   * Cluster has to be started with the --gossip-enable option.
   */
  std::map<std::string, std::string> domainIsolationInfo() const;

  /**
   * Issues an INFO PARTITIONS command to the node's command port to collect
   * information about the LocalLogStore time partitions active on the given
   * shard. The 'level' option is passed directly to the command: '0' = terse,
   * '1' = detailed, '2' = detailed + expensive to collect fields.
   */
  std::vector<std::map<std::string, std::string>>
  partitionsInfo(shard_index_t shard, int level) const;

  /**
   * Issues a INFO SHARD command to the node's command port and compiles
   * a map of dirty shard to dirty time ranges.
   */
  std::map<shard_index_t, RebuildingRangesMetadata> dirtyShardInfo() const;

  // Issues LOGSDB CREATE command. Returns PARTITION_INVALID if it failed.
  partition_id_t createPartition(uint32_t shard);

  Node& setParam(std::string key, std::string value) {
    cmd_args_[key] = value;
    return *this;
  }

  std::vector<std::string> commandLine() const;

  // Folly event-base to be used for Admin API operations
  folly::EventBase event_base_;
};

/**
 * Write to the event log to trigger rebuilding of a shard.
 *
 * @param client Client to use to write to event log.
 * @param node   Node for which to rebuild a shard.
 * @param shard  Shard to rebuild.
 * @param flags  Flags to use.
 * @param rrm    Time ranges for requesting time-ranged rebuilding (aka
 *                    mini rebuilding)
 * @return LSN of the event log record or LSN_INVALID on failure.
 */
lsn_t requestShardRebuilding(Client& client,
                             node_index_t node,
                             uint32_t shard,
                             SHARD_NEEDS_REBUILD_flags_t flags = 0,
                             RebuildingRangesMetadata* rrm = nullptr);

/**
 * Undrain a shard, ie allow the shard to acknowledge rebuilding.
 *
 * @param client  Client to use to write to event log.
 * @param node    Node for which a shard is undrained.
 * @param shard   Shard that is undrained.
 * @return LSN of the event log record or LSN_INVALID on failure.
 */
lsn_t markShardUndrained(Client& client, node_index_t node, uint32_t shard);

/**
 * Mark a shard as unrecoverable in the event log.
 *
 * @param client  Client to use to write to event log.
 * @param node    Node for which a shard is marked unrecoverable.
 * @param shard   Shard that is marked unrecoverable.
 * @return LSN of the event log record or LSN_INVALID on failure.
 */
lsn_t markShardUnrecoverable(Client& client, node_index_t node, uint32_t shard);

/**
 * Wait until some shards have the given state according to the event log.
 * @param client               Client to use for reading the event log.
 * @param shards               Shards for which to check the state.
 * @param st                   Expected authoritative status of the shard.
 * @param wait_for_rebuilding  If true, only return if rebuilding has completed
 *                             (regardless of if it was authoritative), ie all
 *                             donors completed rebuilding.
 * @return  LSN of the last update. Might be more recent than the update that
 *          triggered the state change we're waiting for.
 */
lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       std::set<AuthoritativeStatus> st,
                                       bool wait_for_rebuilding);
lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       AuthoritativeStatus st,
                                       bool wait_for_rebuilding);
lsn_t waitUntilShardHasEventLogState(std::shared_ptr<Client> client,
                                     ShardID shard,
                                     AuthoritativeStatus st,
                                     bool wait_for_rebuilding);

struct SequencerState {
  NodeID node;
  lsn_t last_released_lsn;
  lsn_t next_lsn;
};

/**
 * Executes a GetSeqStateRequest to find out which node is the sequencer
 * for the provided log ID.
 *
 * @param client Client to use to send messages to the cluster nodes
 * @param log_id ID of the lod
 * @param wait_for_recovery Sets eponym option for GetSeqStateRequest
 * @return Result of the GetSeqStateRequest
 */
Status getSeqState(Client* client,
                   logid_t log_id,
                   SequencerState& seq_state,
                   bool wait_for_recovery);

// Returns the default path for logdeviced
std::string defaultLogdevicedPath();

// Returns the default path for ldquery-markdown
std::string defaultMarkdownLDQueryPath();

// Attempts to find a binary, given a relative path to search for.  Within FB
// we just ask the build system for the path. For open source, calls findFile()
std::string findBinary(const std::string& relative_path);

} // namespace IntegrationTestUtils
}} // namespace facebook::logdevice
