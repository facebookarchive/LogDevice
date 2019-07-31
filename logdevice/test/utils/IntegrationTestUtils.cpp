/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/IntegrationTestUtils.h"

#include <cstdio>
#include <ifaddrs.h>
#include <signal.h>
#include <unistd.h>

#include <folly/FileUtil.h>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/Subprocess.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/FileConfigSourceThread.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/NodesConfigurationPublisher.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/SequencerLocatorFactory.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/ops/EventLogUtils.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/ServerInfo.h"
#include "logdevice/test/utils/nc.h"
#include "logdevice/test/utils/port_selection.h"

using facebook::logdevice::configuration::LocalLogsConfig;
#ifdef FB_BUILD_PATHS
#include "common/files/FbcodePaths.h"
#endif

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

#ifdef FB_BUILD_PATHS
std::string defaultLogdevicedPath() {
  return "logdevice/server/logdeviced_nofb";
}
std::string defaultMarkdownLDQueryPath() {
  return "logdevice/ops/ldquery/markdown-ldquery";
}
static const char* CHECKER_PATH =
    "logdevice/replication_checker/replication_checker_nofb";
#else
std::string defaultLogdevicedPath() {
  return "bin/logdeviced";
}
std::string defaultMarkdownLDQueryPath() {
  return "bin/markdown-ldquery";
}
static const char* CHECKER_PATH = "bin/replication_checker_nofb";
#endif

using folly::test::TemporaryDirectory;
namespace fs = boost::filesystem;

// Checks LOGDEVICE_TEST_PAUSE_FOR_GDB environment variable and pauses if
// requested
static void maybe_pause_for_gdb(Cluster&,
                                const std::vector<node_index_t>& indices);
static int dump_file_to_stderr(const char* path);

Cluster::Cluster(std::string root_path,
                 std::unique_ptr<TemporaryDirectory> root_pin,
                 std::string config_path,
                 std::string epoch_store_path,
                 std::string ncs_path,
                 std::string server_binary,
                 std::string cluster_name,
                 bool enable_logsconfig_manager,
                 bool one_config_per_node,
                 dbg::Level default_log_level,
                 bool write_logs_config_file_separately,
                 bool sync_server_config_to_nodes_configuration)
    : root_path_(std::move(root_path)),
      root_pin_(std::move(root_pin)),
      config_path_(std::move(config_path)),
      epoch_store_path_(std::move(epoch_store_path)),
      ncs_path_(std::move(ncs_path)),
      server_binary_(std::move(server_binary)),
      cluster_name_(std::move(cluster_name)),
      enable_logsconfig_manager_(enable_logsconfig_manager),
      one_config_per_node_(one_config_per_node),
      default_log_level_(default_log_level),
      write_logs_config_file_separately_(write_logs_config_file_separately),
      sync_server_config_to_nodes_configuration_(
          sync_server_config_to_nodes_configuration) {
  config_ = std::make_shared<UpdateableConfig>();
  client_settings_.reset(ClientSettings::create());
  ClientSettingsImpl* impl_settings =
      static_cast<ClientSettingsImpl*>(client_settings_.get());
  auto settings_updater = impl_settings->getSettingsUpdater();
  auto updater =
      std::make_shared<TextConfigUpdater>(config_->updateableServerConfig(),
                                          config_->updateableLogsConfig(),
                                          config_->updateableZookeeperConfig(),
                                          impl_settings->getSettings());

  // Client should update its settings from the config file
  auto update_settings = [settings_updater](ServerConfig& config) -> bool {
    auto settings = config.getClientSettingsConfig();

    try {
      settings_updater->setFromConfig(settings);
    } catch (const boost::program_options::error&) {
      return false;
    }
    return true;
  };
  server_config_hook_handles_.push_back(
      config_->updateableServerConfig()->addHook(std::move(update_settings)));
  // Use small polling interval.
  auto config_source =
      std::make_unique<FileConfigSource>(std::chrono::milliseconds(100));
  config_source_ = config_source.get();
  updater->registerSource(std::move(config_source));
  updater->load("file:" + config_path_, nullptr);
  // Config reading shouldn't fail, we just generated it
  ld_check(config_->get() && "Invalid initial config");
  config_->updateableServerConfig()->setUpdater(updater);
  config_->updateableZookeeperConfig()->setUpdater(updater);
  if (!impl_settings->getSettings()->enable_logsconfig_manager) {
    config_->updateableLogsConfig()->setUpdater(updater);
  } else {
    // create initial empty logsconfig
    auto logs_config = std::shared_ptr<LocalLogsConfig>(new LocalLogsConfig());
    logs_config->setInternalLogsConfig(
        config_->getServerConfig()->getInternalLogsConfig());
    config_->updateableLogsConfig()->update(logs_config);
  }

  nodes_configuration_publisher_ =
      std::make_unique<NodesConfigurationPublisher>(
          config_,
          impl_settings->getSettings(),
          std::make_unique<NoopTraceLogger>(config_, folly::none));
}

static std::unique_ptr<TemporaryDirectory> create_temporary_root_dir() {
  return createTemporaryDir("IntegrationTestUtils");
}

logsconfig::LogAttributes
ClusterFactory::createLogAttributesStub(int nstorage_nodes) {
  logsconfig::LogAttributes attrs;
  attrs.set_maxWritesInFlight(256);
  attrs.set_singleWriter(false);
  switch (nstorage_nodes) {
    case 1:
      attrs.set_replicationFactor(1);
      attrs.set_extraCopies(0);
      attrs.set_syncedCopies(0);
      break;
    case 2:
      attrs.set_replicationFactor(2);
      attrs.set_extraCopies(0);
      attrs.set_syncedCopies(0);
      break;
    default:
      attrs.set_replicationFactor(2);
      attrs.set_extraCopies(0);
      attrs.set_syncedCopies(0);
  }
  return attrs;
}

ClusterFactory& ClusterFactory::enableMessageErrorInjection() {
  // defaults
  double chance = 5.0;
  Status msg_status = E::CBREGISTERED;
  std::string env_chance;
  std::string env_status;
  if (getenv_switch("LOGDEVICE_TEST_MESSAGE_ERROR_CHANCE", &env_chance)) {
    double percent = std::stod(env_chance);
    if (percent < 0 || percent > 100) {
      ld_error("LOGDEVICE_TEST_MESSAGE_ERROR_CHANCE environment variable "
               "invalid. Got '%s', but must be between 0 and 100",
               env_chance.c_str());
    } else {
      chance = percent;
    }
  }

  if (getenv_switch("LOGDEVICE_TEST_MESSAGE_STATUS", &env_status)) {
    Status st = errorStrings().reverseLookup<std::string>(
        env_status, [](const std::string& s, const ErrorCodeInfo& e) {
          return s == e.name;
        });
    if (st == errorStrings().invalidEnum()) {
      ld_error("LOGDEVICE_TEST_MESSAGE_STATUS environment variable "
               "invalid. Got '%s'",
               env_status.c_str());
    } else {
      msg_status = st;
    }
  }

  return enableMessageErrorInjection(chance, msg_status);
}

ClusterFactory&
ClusterFactory::setConfigLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("config_log_deltas", attrs);
  setInternalLogAttributes("config_log_snapshots", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setEventLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("event_log_deltas", attrs);
  setInternalLogAttributes("event_log_snapshots", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setEventLogDeltaAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("event_log_deltas", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setMaintenanceLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("maintenance_log_deltas", attrs);
  setInternalLogAttributes("maintenance_log_snapshots", attrs);
  return *this;
}

ClusterFactory& ClusterFactory::enableLogsConfigManager() {
  enable_logsconfig_manager_ = true;
  return *this;
}

logsconfig::LogAttributes
ClusterFactory::createDefaultLogAttributes(int nstorage_nodes) {
  return createLogAttributesStub(nstorage_nodes);
}

std::unique_ptr<Cluster> ClusterFactory::create(int nnodes) {
  Configuration::Nodes nodes;

  std::string loc_prefix = "rg1.dc1.cl1.rw1.rk";

  if (node_configs_.hasValue()) {
    nodes = node_configs_.value();
    if (nnodes == 0) {
      nnodes = (int)nodes.size();
    }
  } else if (hash_based_sequencer_assignment_) {
    // Hash based sequencer assignment is used, all nodes are both sequencers
    // and storage nodes.
    for (int i = 0; i < nnodes; ++i) {
      Configuration::Node node;
      node.name = folly::sformat("server-{}", i);
      node.generation = 1;
      NodeLocation location;
      location.fromDomainString(loc_prefix +
                                std::to_string(i % num_racks_ + 1));
      node.location = location;

      node.addSequencerRole();
      node.addStorageRole(num_db_shards_);

      nodes[i] = std::move(node);
    }
  } else {
    // N0 is the only sequencer node.
    for (int i = 0; i < nnodes; ++i) {
      const bool is_storage_node = (nnodes == 1 || i > 0);
      Configuration::Node node;
      node.name = folly::sformat("server-{}", i);
      NodeLocation location;
      location.fromDomainString(loc_prefix +
                                std::to_string(i % num_racks_ + 1));
      node.location = location;
      node.generation = 1;
      if (i == 0) {
        node.addSequencerRole();
      }
      if (is_storage_node) {
        node.addStorageRole(num_db_shards_);
      }
      nodes[i] = std::move(node);
    }
  }

  ld_check(nnodes == (int)nodes.size());
  for (auto& it : nodes) {
    // this will be overridden later by createOneTry.
    it.second.address =
        Sockaddr(get_localhost_address_str(), std::to_string(it.first));
    if (!no_ssl_address_) {
      it.second.ssl_address.assign(
          Sockaddr(get_localhost_address_str(), std::to_string(it.first)));
    }
  }

  int nstorage_nodes = std::count_if(
      nodes.begin(), nodes.end(), [](Configuration::Nodes::value_type kv) {
        return kv.second.isReadableStorageNode();
      });

  logsconfig::LogAttributes log0;
  if (log_attributes_.hasValue()) {
    // Caller supplied log config, use that.
    log0 = log_attributes_.value();
  } else {
    // Create a default log config with replication parameters that make sense
    // for a cluster of a given size.
    log0 = createDefaultLogAttributes(nstorage_nodes);
  }

  boost::icl::right_open_interval<logid_t::raw_type> logid_interval(
      1, num_logs_ + 1);
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  logs_config->insert(logid_interval, log_group_name_, log0);
  logs_config->markAsFullyLoaded();

  Configuration::NodesConfig nodes_config(std::move(nodes));

  Configuration::MetaDataLogsConfig meta_config;
  if (meta_config_.hasValue()) {
    meta_config = meta_config_.value();
  } else {
    // metadata stored on all storage nodes with max replication factor 3
    meta_config =
        createMetaDataLogsConfig(nodes_config,
                                 nodes_config.getNodes().size(),
                                 internal_logs_replication_factor_ > 0
                                     ? internal_logs_replication_factor_
                                     : 3);
    if (!let_sequencers_provision_metadata_) {
      meta_config.sequencers_write_metadata_logs = false;
      meta_config.sequencers_provision_epoch_store = false;
    }
  }

  // Generic log configuration for internal logs
  logsconfig::LogAttributes internal_log_attrs =
      createLogAttributesStub(nstorage_nodes);
  internal_log_attrs.set_extraCopies(0);

  // Internal logs shouldn't have a lower replication factor than data logs
  if (log_attributes_.hasValue() &&
      log_attributes_.value().replicationFactor().hasValue() &&
      log_attributes_.value().replicationFactor().value() >
          internal_log_attrs.replicationFactor().value()) {
    internal_log_attrs.set_replicationFactor(
        log_attributes_.value().replicationFactor().value());
  }
  if (internal_logs_replication_factor_ > 0) {
    internal_log_attrs.set_replicationFactor(internal_logs_replication_factor_);
  }

  // configure the delta and snapshot logs if the user did not do so already.
  if (event_log_mode_ != EventLogMode::NONE &&
      !internal_logs_.logExists(
          configuration::InternalLogs::EVENT_LOG_DELTAS)) {
    setInternalLogAttributes("event_log_deltas", internal_log_attrs);
  }
  if (event_log_mode_ == EventLogMode::SNAPSHOTTED &&
      !internal_logs_.logExists(
          configuration::InternalLogs::EVENT_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("event_log_snapshots", internal_log_attrs);
  }

  // configure the delta and snapshot logs for logsconfig
  // if the user did not do so already.
  if (!internal_logs_.logExists(
          configuration::InternalLogs::CONFIG_LOG_DELTAS)) {
    setInternalLogAttributes("config_log_deltas", internal_log_attrs);
  }

  if (!internal_logs_.logExists(
          configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("config_log_snapshots", internal_log_attrs);
  }

  // configure the delta and snapshot logs for Maintenance log
  // if the user did not do so already
  if (!internal_logs_.logExists(
          configuration::InternalLogs::MAINTENANCE_LOG_DELTAS)) {
    setInternalLogAttributes("maintenance_log_deltas", internal_log_attrs);
  }

  if (!internal_logs_.logExists(
          configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("maintenance_log_snapshots", internal_log_attrs);
  }

  // Have all connections assigned to the ROOT scope and use the same
  // shaping config.
  configuration::TrafficShapingConfig ts_config;
  configuration::ShapingConfig read_throttling_config(
      std::set<NodeLocationScope>{NodeLocationScope::NODE},
      std::set<NodeLocationScope>{NodeLocationScope::NODE});
  if (use_default_traffic_shaping_config_) {
    auto root_fgp = ts_config.flowGroupPolicies.find(NodeLocationScope::ROOT);
    ld_check(root_fgp != ts_config.flowGroupPolicies.end());
    root_fgp->second.setConfigured(true);
    root_fgp->second.setEnabled(true);
    // Set burst capacity small to increase the likelyhood of experiencing
    // a message deferral during a test run.
    root_fgp->second.set(Priority::MAX,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    root_fgp->second.set(Priority::CLIENT_HIGH,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000,
                         /*Max Bps*/ 2000000);
    // Provide 0 capacity for client normal so that it must always be
    // deferred to a priority queue run.
    root_fgp->second.set(Priority::CLIENT_NORMAL,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 0,
                         /*Max Bps*/ 1000000);
    root_fgp->second.set(Priority::CLIENT_LOW,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    root_fgp->second.set(Priority::BACKGROUND,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    root_fgp->second.set(FlowGroup::PRIORITYQ_PRIORITY,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    auto read_fgp =
        read_throttling_config.flowGroupPolicies.find(NodeLocationScope::NODE);
    ld_check(read_fgp != read_throttling_config.flowGroupPolicies.end());
    read_fgp->second.setConfigured(true);
    read_fgp->second.setEnabled(true);
    read_fgp->second.set(static_cast<Priority>(Priority::MAX),
                         /*Burst Bytes*/ 25000,
                         /*Guaranteed Bps*/ 50000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_HIGH),
                         /*Burst Bytes*/ 20000,
                         /*Guaranteed Bps*/ 40000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_NORMAL),
                         /*Burst Bytes*/ 15000,
                         /*Guaranteed Bps*/ 30000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_LOW),
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 20000);
  }

  auto server_settings = ServerConfig::SettingsConfig();
  auto client_settings = ServerConfig::SettingsConfig();
  if (!enable_logsconfig_manager_) {
    // Default is true, so we only set to false if this option is not set.
    server_settings["enable-logsconfig-manager"] =
        client_settings["enable-logsconfig-manager"] = "false";
  }

  client_settings["event-log-snapshotting"] = "false";
  server_settings["event-log-snapshotting"] = "false";

  if (!no_ssl_address_) {
    client_settings["ssl-ca-path"] =
        TEST_SSL_FILE("logdevice_test_valid_ca.cert");
  }

  auto config = std::make_unique<Configuration>(
      ServerConfig::fromDataTest(cluster_name_,
                                 std::move(nodes_config),
                                 std::move(meta_config),
                                 ServerConfig::PrincipalsConfig(),
                                 ServerConfig::SecurityConfig(),
                                 ServerConfig::TraceLoggerConfig(),
                                 std::move(ts_config),
                                 std::move(read_throttling_config),
                                 std::move(server_settings),
                                 std::move(client_settings),
                                 internal_logs_),
      enable_logsconfig_manager_ ? nullptr : logs_config);
  logs_config->setInternalLogsConfig(
      config->serverConfig()->getInternalLogsConfig());

  if (getenv_switch("LOGDEVICE_TEST_USE_TCP")) {
    ld_info("LOGDEVICE_TEST_USE_TCP environment variable is set. Using TCP "
            "ports instead of unix domain sockets.");
    use_tcp_ = true;
  }

  return create(*config);
}

std::unique_ptr<Cluster>
ClusterFactory::createOneTry(const Configuration& source_config) {
  const std::string actual_server_binary = actualServerBinary();
  if (actual_server_binary.empty()) {
    // Abort early if this failed
    return nullptr;
  }

  Configuration::Nodes nodes = source_config.serverConfig()->getNodes();
  const int nnodes = nodes.size();
  std::vector<node_index_t> node_ids(nnodes);
  std::map<node_index_t, node_gen_t> replacement_counters;

  int j = 0;
  for (auto it : nodes) {
    ld_check(j < nnodes);
    node_ids[j++] = it.first;
    replacement_counters[it.first] = it.second.generation;
  }
  ld_check(j == nnodes);

  std::string root_path;
  std::unique_ptr<TemporaryDirectory> root_pin;
  if (root_path_.hasValue()) {
    root_path = root_path_.value();
    boost::filesystem::create_directories(root_path);
  } else {
    // Create a directory that will contain all the data for this cluster
    root_pin = create_temporary_root_dir();
    root_path = root_pin->path().string();
  }

  std::string epoch_store_path = root_path + "/epoch_store";
  mkdir(epoch_store_path.c_str(), 0777);

  std::string ncs_path;
  {
    // If the settings specify a certain NCS path, use it, otherwise, use a
    // default one under root_path.
    const auto& server_settings =
        source_config.serverConfig()->getServerSettingsConfig();
    auto config_ncs_path =
        server_settings.find("nodes-configuration-file-store-dir");
    if (config_ncs_path != server_settings.end()) {
      ncs_path = config_ncs_path->second;
    } else {
      ncs_path = root_path + "/nc_store";
      mkdir(ncs_path.c_str(), 0777);
    }
  }

  // Each node in the cluster has a SockaddrPair object that defines with
  // tcp port or unix domain socket it uses for its protocol port and
  // command port.
  std::vector<SockaddrPair> addrs(nnodes);
  std::vector<SockaddrPair> ssl_addrs(nnodes);

  if (use_tcp_) {
    // This test uses TCP. Look for N free pairs of ports
    std::vector<detail::PortOwnerPtrTuple> port_pairs;
    if (detail::find_free_port_set(nnodes * 3, port_pairs) != 0) {
      ld_error("Not enough free ports on system to start LogDevice test "
               "cluster with %d nodes",
               nnodes);
      return nullptr;
    }

    for (int i = 0; i < nnodes; ++i) {
      auto& node = nodes[node_ids[i]];
      addrs[i] = SockaddrPair::fromTcpPortPair(std::move(port_pairs[i]));
      SockaddrPair::buildGossipTcpSocket(
          addrs[i], std::get<0>(port_pairs[nnodes + i])->port);
      ssl_addrs[i] =
          SockaddrPair::fromTcpPortPair(std::move(port_pairs[2 * nnodes + i]));
      node.address = addrs[i].protocol_addr_;
      node.gossip_address = addrs[i].gossip_addr_;
      if (!no_ssl_address_) {
        node.ssl_address.assign(ssl_addrs[i].protocol_addr_);
      }
    }
  } else {
    // This test uses unix domain sockets. These will be created in the
    // test directory.
    for (int i = 0; i < nnodes; ++i) {
      auto& node = nodes[node_ids[i]];
      addrs[i] = SockaddrPair::buildUnixSocketPair(
          Cluster::getNodeDataPath(
              root_path, node_ids[i], replacement_counters[node_ids[i]]),
          false);
      ssl_addrs[i] = SockaddrPair::buildUnixSocketPair(
          Cluster::getNodeDataPath(
              root_path, node_ids[i], replacement_counters[node_ids[i]]),
          true);
      node.address = addrs[i].protocol_addr_;
      node.gossip_address = addrs[i].gossip_addr_;
      if (!no_ssl_address_) {
        node.ssl_address.assign(ssl_addrs[i].protocol_addr_);
      }
    }
  }

  ld_info("Cluster created with data in %s", root_path.c_str());

  Configuration::NodesConfig nodes_config(std::move(nodes));
  std::unique_ptr<Configuration> config = std::make_unique<Configuration>(
      source_config.serverConfig()->withNodes(nodes_config),
      source_config.logsConfig());

  // Write new config to disk so that logdeviced processes can access it
  std::string config_path = root_path + "/logdevice.conf";
  if (overwriteConfig(config_path.c_str(),
                      config->serverConfig().get(),
                      config->logsConfig().get(),
                      write_logs_config_file_separately_) != 0) {
    return nullptr;
  }

  std::unique_ptr<Cluster> cluster(
      new Cluster(root_path,
                  std::move(root_pin),
                  config_path,
                  epoch_store_path,
                  ncs_path,
                  actual_server_binary,
                  cluster_name_,
                  enable_logsconfig_manager_,
                  one_config_per_node_,
                  default_log_level_,
                  write_logs_config_file_separately_,
                  sync_server_config_to_nodes_configuration_));
  if (use_tcp_) {
    cluster->use_tcp_ = true;
  }
  if (no_ssl_address_) {
    cluster->no_ssl_address_ = true;
  }

  cluster->outer_tries_ = outerTries();
  cluster->cmd_param_ = cmd_param_;
  cluster->num_db_shards_ = num_db_shards_;
  cluster->rocksdb_type_ = rocksdb_type_;
  cluster->hash_based_sequencer_assignment_ = hash_based_sequencer_assignment_;
  cluster->setNodeReplacementCounters(std::move(replacement_counters));
  cluster->maintenance_manager_node_ = maintenance_manager_node_;

  if (cluster->rocksdb_type_ == RocksDBType::SINGLE) {
    cluster->setParam(
        "--rocksdb-partitioned", "false", ParamScope::STORAGE_NODE);
  }

  // create Node objects, but don't start the processes
  for (int i = 0; i < nnodes; i++) {
    cluster->nodes_[node_ids[i]] = cluster->createNode(
        node_ids[i], std::move(addrs[i]), std::move(ssl_addrs[i]));
  }

  // if allowed, provision the initial epoch metadata in epoch store,
  // as well as metadata storage nodes
  if (provision_epoch_metadata_) {
    if (cluster->provisionEpochMetaData(
            provision_nodeset_selector_, allow_existing_metadata_) != 0) {
      return nullptr;
    }
  }

  if (provision_nodes_configuration_store_) {
    const auto& server_config = config->serverConfig();
    if (cluster->updateNodesConfigurationFromServerConfig(
            server_config.get()) != 0) {
      return nullptr;
    }
  }

  if (!defer_start_ && cluster->start() != 0) {
    return nullptr;
  }

  if (num_logs_config_manager_logs_ > 0) {
    auto log_group = createLogsConfigManagerLogs(cluster);
    if (log_group == nullptr) {
      ld_error("Failed to create the default logs config manager logs.");
    }
  }

  return cluster;
}

std::unique_ptr<client::LogGroup>
ClusterFactory::createLogsConfigManagerLogs(std::unique_ptr<Cluster>& cluster) {
  auto nodes = cluster->getConfig()->getServerConfig()->getNodes();
  int num_storage_nodes = 0;
  for (const auto& node : nodes) {
    if (node.second.isReadableStorageNode()) {
      num_storage_nodes++;
    }
  }
  logsconfig::LogAttributes attrs = log_attributes_.hasValue()
      ? log_attributes_.value()
      : createDefaultLogAttributes(num_storage_nodes);

  return cluster->createClient()->makeLogGroupSync(
      "/test_logs",
      logid_range_t(logid_t(1), logid_t(num_logs_config_manager_logs_)),
      attrs);
}

int Cluster::expand(std::vector<node_index_t> new_indices, bool start_nodes) {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);
  Configuration::Nodes nodes = config_->get()->serverConfig()->getNodes();

  std::sort(new_indices.begin(), new_indices.end());
  if (std::unique(new_indices.begin(), new_indices.end()) !=
      new_indices.end()) {
    ld_error("expand() called with duplicate indices");
    return -1;
  }
  for (node_index_t i : new_indices) {
    if (nodes.count(i) || nodes_.count(i)) {
      ld_error(
          "expand() called with node index %d that already exists", (int)i);
      return -1;
    }
  }

  for (node_index_t idx : new_indices) {
    Configuration::Node node;
    node.name = folly::sformat("server-{}", idx);
    node.generation = 1;
    setNodeReplacementCounter(idx, 1);

    // Storage only node.
    node.addStorageRole(num_db_shards_);
    nodes[idx] = std::move(node);
  }

  std::vector<SockaddrPair> addrs(new_indices.size());
  std::vector<SockaddrPair> gossip_addrs(new_indices.size());
  std::vector<SockaddrPair> ssl_addrs(new_indices.size());

  if (use_tcp_) {
    // This test uses TCP. Look for N free pairs of ports
    std::vector<detail::PortOwnerPtrTuple> port_pairs;
    if (detail::find_free_port_set(new_indices.size() * 3, port_pairs) != 0) {
      ld_error("Not enough free ports on system to expand LogDevice test "
               "cluster with %lu nodes",
               new_indices.size());
      return -1;
    }

    for (int i = 0; i < new_indices.size(); ++i) {
      addrs[i] = SockaddrPair::fromTcpPortPair(std::move(port_pairs[i]));
      SockaddrPair::buildGossipTcpSocket(
          addrs[i], std::get<0>(port_pairs[new_indices.size() + i])->port);
      ssl_addrs[i] = SockaddrPair::fromTcpPortPair(
          std::move(port_pairs[2 * new_indices.size() + i]));
    }
  } else {
    // This test uses unix domain sockets. These will be created in the
    // test directory.
    for (int i = 0; i < new_indices.size(); ++i) {
      int idx = new_indices[i];
      addrs[i] = SockaddrPair::buildUnixSocketPair(
          Cluster::getNodeDataPath(root_path_, idx), false);
      gossip_addrs[i] = SockaddrPair::buildGossipSocket(
          Cluster::getNodeDataPath(root_path_, idx));
      ssl_addrs[i] = SockaddrPair::buildUnixSocketPair(
          Cluster::getNodeDataPath(root_path_, idx), true);
    }
  }

  for (size_t i = 0; i < new_indices.size(); ++i) {
    node_index_t idx = new_indices[i];
    nodes[idx].address = addrs[i].protocol_addr_;
    nodes[idx].gossip_address = addrs[i].gossip_addr_;
    if (!no_ssl_address_) {
      nodes[idx].ssl_address = ssl_addrs[i].protocol_addr_;
    }
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto config = config_->get();
  auto new_server_config =
      config->serverConfig()->withNodes(std::move(nodes_config));
  int rv = writeConfig(new_server_config.get(), config->logsConfig().get());
  if (rv != 0) {
    return -1;
  }
  waitForConfigUpdate();

  if (!start_nodes) {
    return 0;
  }

  for (size_t i = 0; i < new_indices.size(); ++i) {
    node_index_t idx = new_indices[i];
    nodes_[idx] = createNode(idx, std::move(addrs[i]), std::move(ssl_addrs[i]));
  }

  return start(new_indices);
}

int Cluster::expand(int nnodes, bool start) {
  std::vector<node_index_t> new_indices;
  node_index_t first = config_->getNodesConfiguration()->getMaxNodeIndex() + 1;
  for (int i = 0; i < nnodes; ++i) {
    new_indices.push_back(first + i);
  }
  return expand(new_indices, start);
}

int Cluster::shrink(std::vector<node_index_t> indices) {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);

  if (indices.empty()) {
    ld_error("shrink() called with no nodes");
    return -1;
  }

  Configuration::Nodes nodes = config_->get()->serverConfig()->getNodes();

  std::sort(indices.begin(), indices.end());
  if (std::unique(indices.begin(), indices.end()) != indices.end()) {
    ld_error("shrink() called with duplicate indices");
    return -1;
  }
  for (node_index_t i : indices) {
    if (!nodes.count(i) || !nodes_.count(i)) {
      ld_error("shrink() called with node index %d that doesn't exist", (int)i);
      return -1;
    }
  }
  if (indices.size() >= std::min(nodes.size(), nodes_.size())) {
    ld_error("Cannot remove all nodes from the cluster");
    return -1;
  }

  // Kill the nodes before we remove them from the cluster.
  for (node_index_t i : indices) {
    if (getNode(i).isRunning()) {
      getNode(i).kill();
    }
  }

  for (node_index_t i : indices) {
    nodes_.erase(i);
    nodes.erase(i);
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto config = config_->get();
  auto new_server_config =
      config->serverConfig()->withNodes(std::move(nodes_config));
  int rv = writeConfig(new_server_config.get(), config->logsConfig().get());
  if (rv != 0) {
    return -1;
  }
  waitForConfigUpdate();

  return 0;
}

int Cluster::shrink(int nnodes) {
  auto cfg = config_->get();

  // Find nnodes highest node indices.
  std::vector<node_index_t> indices;
  for (auto it = nodes_.crbegin(); it != nodes_.crend() && nnodes > 0;
       ++it, --nnodes) {
    indices.push_back(it->first);
  }
  if (nnodes != 0) {
    ld_error("shrink() called with too many nodes");
    return -1;
  }

  return shrink(indices);
}

void Cluster::stop() {
  for (auto& it : nodes_) {
    it.second->kill();
  }
}

int Cluster::start(std::vector<node_index_t> indices) {
  if (indices.size() == 0) {
    for (auto& it : nodes_) {
      indices.push_back(it.first);
    }
  }

  for (node_index_t i : indices) {
    nodes_.at(i)->start();
  }

  // If using TCP, give the cluster a few seconds to start up before failing
  // and retrying with a different set of ports.
  std::chrono::steady_clock::time_point deadline = use_tcp_
      ? std::chrono::steady_clock::now() + start_timeout_
      : std::chrono::steady_clock::time_point::max();

  for (node_index_t i : indices) {
    if (nodes_.at(i)->waitUntilStarted(deadline) != 0) {
      return -1;
    }
    if (hash_based_sequencer_assignment_ &&
        nodes_.at(i)->waitUntilAvailable(deadline) != 0) {
      return -1;
    }
  }

  maybe_pause_for_gdb(*this, indices);
  return 0;
}

int Cluster::provisionEpochMetaData(std::shared_ptr<NodeSetSelector> selector,
                                    bool allow_existing_metadata) {
  auto meta_provisioner = createMetaDataProvisioner();
  if (selector == nullptr) {
    selector = NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL);
  }

  int rv = meta_provisioner->provisionEpochMetaData(
      std::move(selector), allow_existing_metadata, true);
  if (rv != 0) {
    ld_error("Failed to provision epoch metadata for the cluster.");
  }
  return rv;
}

int Cluster::updateNodesConfigurationFromServerConfig(
    const ServerConfig* server_config) {
  using namespace logdevice::configuration::nodes;
  auto nc = NodesConfigLegacyConverter::fromLegacyNodesConfig(
      server_config->getNodesConfig(),
      server_config->getMetaDataLogsConfig(),
      server_config->getVersion());
  NodesConfigurationStoreFactory::Params params;
  params.type = NodesConfigurationStoreFactory::NCSType::File;
  params.file_store_root_dir = ncs_path_;
  params.path = NodesConfigurationStoreFactory::getDefaultConfigStorePath(
      NodesConfigurationStoreFactory::NCSType::File, cluster_name_);

  auto store = NodesConfigurationStoreFactory::create(std::move(params));
  if (store == nullptr) {
    return -1;
  }
  auto serialized = NodesConfigurationCodec::serialize(*nc);
  if (serialized.empty()) {
    return -1;
  }
  store->updateConfigSync(std::move(serialized), folly::none);
  return 0;
}

std::unique_ptr<Node> Cluster::createNode(node_index_t index,
                                          SockaddrPair addrs,
                                          SockaddrPair ssl_addrs) const {
  std::shared_ptr<const Configuration> config = config_->get();
  ld_check(config != nullptr);

  std::unique_ptr<Node> node = std::make_unique<Node>();
  node->node_index_ = index;
  node->addrs_ = std::move(addrs);
  node->ssl_addrs_ = std::move(ssl_addrs);
  node->num_db_shards_ = num_db_shards_;
  node->rocksdb_type_ = rocksdb_type_;
  node->server_binary_ = server_binary_;

  // Data path will be something like
  // /tmp/logdevice/IntegrationTestUtils.MkkZyS/N0:1/
  node->data_path_ = Cluster::getNodeDataPath(root_path_, index);
  boost::filesystem::create_directories(node->data_path_);

  if (one_config_per_node_) {
    // create individual config file for each node under their own directory
    node->config_path_ = node->data_path_ + "/logdevice.conf";
    overwriteConfigFile(node->config_path_.c_str(), config_->get()->toString());
  } else {
    node->config_path_ = config_path_;
  }

  node->cmd_args_ = commandArgsForNode(index, *node);

  ld_info("Node N%d:%d will be started on protocol_addr:%s"
          ", gossip_addr:%s, command_addr:%s, admin_addr:%s (data in %s)",
          index,
          getNodeReplacementCounter(index),
          node->addrs_.protocol_addr_.toString().c_str(),
          node->addrs_.gossip_addr_.toString().c_str(),
          node->addrs_.command_addr_.toString().c_str(),
          node->addrs_.admin_addr_.toString().c_str(),
          node->data_path_.c_str());

  return node;
}

ParamMap Cluster::commandArgsForNode(node_index_t i, const Node& node) const {
  std::shared_ptr<const Configuration> config = config_->get();

  const auto& p = node.addrs_.protocol_addr_;
  auto protocol_addr_param = p.isUnixAddress()
      ? std::make_pair("--unix-socket", ParamValue{p.getPath()})
      : std::make_pair("--port", ParamValue{std::to_string(p.port())});

  const auto& c = node.addrs_.command_addr_;
  auto command_addr_param = c.isUnixAddress()
      ? std::make_pair("--command-unix-socket", ParamValue{c.getPath()})
      : std::make_pair("--command-port", ParamValue{std::to_string(c.port())});

  const auto& admn = node.addrs_.admin_addr_;
  auto admin_addr_param = admn.isUnixAddress()
      ? std::make_pair("--admin-unix-socket", ParamValue{admn.getPath()})
      : std::make_pair("--admin-port", ParamValue{std::to_string(admn.port())});

  // clang-format off

  // Construct the default parameters.
  ParamMaps default_param_map = {
    { ParamScope::ALL,
      {
        protocol_addr_param, command_addr_param, admin_addr_param,
        {"--test-mode", ParamValue{"true"}},
        {"--config-path", ParamValue{"file:" + node.config_path_}},
        {"--epoch-store-path", ParamValue{epoch_store_path_}},
        {"--nodes-configuration-file-store-dir", ParamValue{ncs_path_}},
        // Poll for config updates more frequently in tests so that they
        // progress faster
        {"--file-config-update-interval", ParamValue{"100ms"}},
        {"--loglevel", ParamValue{loglevelToString(default_log_level_)}},
        {"--log-file", ParamValue{node.getLogPath()}},
        {"--server-id", ParamValue{node.server_id_}},
        {"--assert-on-data", ParamValue{}},
        {"--enable-config-synchronization", ParamValue{}},
        // Disable rebuilding by default in tests, the test framework
        // (`waitUntilRebuilt' etc) is not ready for it: #14697277
        {"--disable-rebuilding", ParamValue{"true"}},
        // Disable the random delay for SHARD_IS_REBUILT messages
        {"--shard-is-rebuilt-msg-delay", ParamValue{"0s..0s"}},
        // TODO(T22614431): remove this option once it's been enabled
        // everywhere.
        {"--allow-conditional-rebuilding-restarts", ParamValue{"true"}},
        {"--rebuilding-restarts-grace-period", ParamValue{"1ms"}},
        {"--planner-scheduling-delay", ParamValue{"1s"}},
        // RebuildingTest does not expect this: #14697312
        {"--enable-self-initiated-rebuilding", ParamValue{"false"}},
        // disable failure detector because it delays sequencer startup
        {"--gossip-enabled", ParamValue{"false"}},
        {"--ignore-cluster-marker", ParamValue{"true"}},
        {"--rocksdb-auto-create-shards", ParamValue{"true"}},
        {"--num-workers", ParamValue{"5"}},
        {"--enable-maintenance-manager",
          ParamValue{i == maintenance_manager_node_?"true" : "false"}},
      }
    },
    { ParamScope::SEQUENCER,
      {
        {"--sequencers", ParamValue{"all"}},
        // Small timeout is needed so that appends that happen right after
        // rebuilding, when socket isn't reconnected yet, retry quickly.
        {"--store-timeout", ParamValue{"10ms..1s"}},
        // smaller recovery retry timeout for reading seqencer metadata
        {"--recovery-seq-metadata-timeout", ParamValue{"100ms..500ms"}},
        // if we fail to store something on a node, we should retry earlier than
        // the default 60s
        {"--unroutable-retry-interval", ParamValue{"1s"}},
      }
    },
    { ParamScope::STORAGE_NODE,
      {
        {"--local-log-store-path", ParamValue{node.getDatabasePath()}},
        // Disable disk space checking by default; tests that want it can
        // override
        {"--free-disk-space-threshold", ParamValue{"0.000001"}},
        // Run fewer than the default 4 threads to perform better under load
        {"--storage-threads-per-shard-slow", ParamValue{"2"}},
        {"--rocksdb-allow-fallocate", ParamValue{"false"}},
        // Reduce memory usage for storage thread queues compared to the
        // default setting
        {"--max-inflight-storage-tasks", ParamValue{"512"}},
      }
    }
  };

  // TODO(tau0) Remove this.
  if (enable_logsconfig_manager_) {
    default_param_map[ParamScope::ALL]
      ["--enable-logsconfig-manager"] = ParamValue{"true"};
  } else {
    default_param_map[ParamScope::ALL]
      ["--enable-logsconfig-manager"] = ParamValue{"false"};
  }

  default_param_map[ParamScope::ALL]["--enable-nodes-configuration-manager"] = (
      enable_ncm_ ? ParamValue{"true"} : ParamValue{"false"});

  if (!no_ssl_address_) {
    default_param_map[ParamScope::ALL]["--ssl-ca-path"] =
        ParamValue(TEST_SSL_FILE("logdevice_test_valid_ca.cert"));
    default_param_map[ParamScope::ALL]["--ssl-cert-path"] =
        ParamValue(TEST_SSL_FILE("logdevice_test_valid.cert"));
    default_param_map[ParamScope::ALL]["--ssl-key-path"] =
        ParamValue(TEST_SSL_FILE("logdevice_test.key"));
  }

  // clang-format on

  // Both `default_param_map' and `cmd_param_' specify params for the 3
  // different ParamScopes (ALL, SEQUENCER, STORAGE_NODE).  Time to flatten
  // based on whether the current node is a sequencer and/or a storage node.

  std::vector<ParamScope> scopes;
  if (config->serverConfig()->getNode(i)->isSequencingEnabled()) {
    scopes.push_back(ParamScope::SEQUENCER);
  }
  if (config->serverConfig()->getNode(i)->isReadableStorageNode()) {
    scopes.push_back(ParamScope::STORAGE_NODE);
  }
  // ALL comes last so it doesn't overwrite more specific scopes.
  // (unordered_map::insert() keeps existing keys.)
  scopes.push_back(ParamScope::ALL);
  ParamMap defaults_flat, overrides_flat;
  for (ParamScope scope : scopes) {
    defaults_flat.insert(
        default_param_map[scope].cbegin(), default_param_map[scope].cend());
    auto it = cmd_param_.find(scope);
    if (it != cmd_param_.end()) {
      overrides_flat.insert(it->second.cbegin(), it->second.cend());
    }
  }

  // Now we can build the final params map and argv.
  ParamMap final_params;
  // Inserting overrides first so they take precedence over defaults.
  final_params.insert(overrides_flat.cbegin(), overrides_flat.cend());
  final_params.insert(defaults_flat.cbegin(), defaults_flat.cend());

  return final_params;
}

void Cluster::partition(std::vector<std::set<int>> partitions) {
  // one_config_per_node_ is required
  ld_check(one_config_per_node_);

  // for every node in a partition, update the address of nodes outside
  // the partition to a non-existent unix socket. this effectively create a
  // virtual network partition.
  for (auto p : partitions) {
    Configuration::Nodes nodes = getConfig()->get()->serverConfig()->getNodes();

    for (auto& n : nodes) {
      if (p.find(n.first) == p.end()) {
        // node is outside the partition, update its address to unreachable
        // unix socket (to trigger sender reload on config update)
        std::string addr = "/nonexistent" + folly::to<std::string>(n.first);
        n.second.address = Sockaddr(addr);
        n.second.gossip_address = Sockaddr(addr);
      }
    }

    Configuration::NodesConfig nodes_config(std::move(nodes));
    auto old_config = config_->get();
    Configuration config(old_config->serverConfig()->withNodes(nodes_config),
                         old_config->logsConfig());
    for (auto i : p) {
      // replace config file of all the nodes within that partition.
      overwriteConfigFile(getNode(i).config_path_.c_str(), config.toString());
    }
  }
}

void Cluster::waitUntilAll(const char* desc, std::function<bool(Node&)> pred) {
  wait_until(desc, [&]() {
    return std::all_of(getNodes().begin(), getNodes().end(), [=](auto& p) {
      return pred(*p.second);
    });
  });
}

std::unique_ptr<Cluster>
ClusterFactory::create(const Configuration& source_config) {
  for (int outer_try = 0; outer_try < outerTries(); ++outer_try) {
    std::unique_ptr<Cluster> cluster = createOneTry(source_config);
    if (cluster) {
      return cluster;
    }
    // Cluster failed to start.  This may be because of an actual failure or a
    // port race (someone acquired the port between when we released it and
    // the new process tried to listen on it).  Retry in case it was a port
    // race.
  }

  ld_critical(
      "Failed to start LogDevice test cluster after %d tries", outerTries());
  throw std::runtime_error("Failed to start LogDevice test cluster");
}

Node::Node() {
  const char* alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
  uint32_t alphabet_size = (uint32_t)strlen(alphabet);
  while (server_id_.size() < 10) {
    server_id_ += alphabet[folly::Random::rand32(alphabet_size)];
  }
}

void Node::start() {
  folly::Subprocess::Options options;
  options.parentDeathSignal(SIGKILL); // kill children if test process dies

  // Make any tcp port that we reserved available to logdeviced.
  std::get<0>(addrs_.tcp_ports_owned_).reset();
  std::get<1>(addrs_.tcp_ports_owned_).reset();
  std::get<2>(addrs_.tcp_ports_owned_).reset();
  std::get<0>(ssl_addrs_.tcp_ports_owned_).reset();
  std::get<1>(ssl_addrs_.tcp_ports_owned_).reset();
  std::get<2>(ssl_addrs_.tcp_ports_owned_).reset();

  // Without this, calling start() twice would causes a crash because
  // folly::Subprocess::~Subprocess asserts that the process is not running.
  if (isRunning()) {
    // The node is already started.
    return;
  }

  ld_info("Node N%d Command Line: %s",
          node_index_,
          folly::join(" ", commandLine()).c_str());

  ld_info("Starting node %d", node_index_);
  logdeviced_.reset(new folly::Subprocess(commandLine(), options));
  ld_info("Started node %d", node_index_);

  stopped_ = false;
}

void Node::restart(bool graceful, bool wait_until_available) {
  if (graceful) {
    int ret = shutdown();
    ld_check(0 == ret);
  } else {
    kill();
  }
  start();
  if (wait_until_available) {
    waitUntilAvailable();
  }
}

std::vector<std::string> Node::commandLine() const {
  std::vector<std::string> argv = {server_binary_};
  for (const auto& pair : cmd_args_) {
    argv.emplace_back(pair.first);
    if (pair.second.hasValue()) {
      argv.emplace_back(pair.second.value());
    }
  }
  return argv;
}

int Node::shutdown() {
  if (isRunning()) {
    sendCommand("quit");
    return waitUntilExited();
  }
  return 0;
}

bool Node::isRunning() const {
  return logdeviced_ && logdeviced_->returnCode().running() &&
      logdeviced_->poll().running();
}

void Node::kill() {
  if (isRunning()) {
    ld_info("Killing node on %s", addrs_.protocol_addr_.toString().c_str());
    logdeviced_->kill();
    logdeviced_->wait();
    ld_info("Killed node on %s", addrs_.protocol_addr_.toString().c_str());
    stopped_ = true;
  }
  logdeviced_.reset();
}

void Node::wipeShard(uint32_t shard) {
  std::string shard_name = "shard" + folly::to<std::string>(shard);
  std::string db_path = getDatabasePath();
  auto shard_path = fs::path(db_path) / shard_name;
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
}

std::string Node::sendCommand(const std::string& command, bool ssl) const {
  std::string error;
  std::string response = test::nc(
      addrs_.command_addr_.getSocketAddress(), command + "\r\n", &error, ssl);
  if (!error.empty()) {
    ld_debug("Failed to send command: %s", error.c_str());
  }
  ld_debug(
      "Received response to \"%s\": %s", command.c_str(), response.c_str());
  return response;
}

folly::SocketAddress Node::getAdminAddress() const {
  return addrs_.admin_addr_.getSocketAddress();
}

std::string Node::sendIfaceCommand(const std::string& command,
                                   const std::string ifname) const {
  std::string ifaceAddr = getIfaceAddr(ifname);
  if (ifaceAddr.empty()) {
    ld_error("Failed to send command, couldn't get interface address!");
    return "";
  }
  folly::SocketAddress iface = addrs_.command_addr_.getSocketAddress();
  iface.setFromIpPort(ifaceAddr, iface.getPort());
  std::string error;
  std::string response = test::nc(iface, command + "\r\n", &error);
  if (!error.empty()) {
    ld_debug("Failed to send command: %s", error.c_str());
  }
  return response;
}

std::string Node::getIfaceAddr(const std::string ifname) const {
  struct ifaddrs *ifa, *ifa_tmp;

  if (getifaddrs(&ifa) == -1) {
    ld_error("getifaddrs failed");
    return "";
  }
  SCOPE_EXIT {
    freeifaddrs(ifa);
  };

  for (ifa_tmp = ifa; ifa_tmp; ifa_tmp = ifa_tmp->ifa_next) {
    if (ifa_tmp->ifa_addr && strcmp(ifa_tmp->ifa_name, ifname.c_str()) == 0 &&
        (ifa_tmp->ifa_addr->sa_family == AF_INET6 ||
         ifa_tmp->ifa_addr->sa_family == AF_INET)) {
      if (ifa_tmp->ifa_addr->sa_family == AF_INET6) { // IPv6
        struct sockaddr_in6* in6 = (struct sockaddr_in6*)ifa_tmp->ifa_addr;
        char addr[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &in6->sin6_addr, addr, INET6_ADDRSTRLEN);
        return std::string(addr);
      } else { // AF_INET = IPv4
        struct sockaddr_in* in4 = (struct sockaddr_in*)ifa_tmp->ifa_addr;
        char addr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &in4->sin_addr, addr, INET_ADDRSTRLEN);
        return std::string(addr);
      }
    }
  }
  ld_error("Failed to find interface %s", ifname.c_str());
  return "";
}

folly::Optional<test::ServerInfo> Node::getServerInfo() const {
  auto data = sendCommand("info --json");
  if (data.empty()) {
    return folly::Optional<test::ServerInfo>();
  }
  return folly::Optional<test::ServerInfo>(test::ServerInfo::fromJson(data));
}

int Node::waitUntilStarted(std::chrono::steady_clock::time_point deadline) {
  ld_info("Waiting for node %d to start", node_index_);
  bool died = false;

  // If we wait for a long time, dump the server's error log file to stderr to
  // help debug.
  auto t1 = std::chrono::steady_clock::now();
  bool should_dump_log = dbg::currentLevel >= dbg::Level::WARNING;

  auto started = [this, &died, &should_dump_log, t1]() {
    // no need to wait if the process is not even running
    died = !isRunning();
    if (died) {
      return true;
    }
    // To verify if the server has started, send an INFO admin command and see
    // if the server id matches what we expect.  This is to detect races where
    // two tests try to simultaneously claim the same port and hand it over to
    // a child process.
    // catch any exceptions here and ignore them. eg: socket closed while
    // querying the server. logdeviced will eventually be restarted.
    try {
      folly::Optional<test::ServerInfo> info = getServerInfo();
      if (info) {
        bool match = info->server_id == server_id_;
        if (!match) {
          ld_warning("Server process is running but its --server-id \"%s\" "
                     "does not match the expected \"%s\"",
                     info->server_id.c_str(),
                     server_id_.c_str());
        }
        return match;
      }
    } catch (...) {
    }

    auto t2 = std::chrono::steady_clock::now();
    if (should_dump_log && t2 - t1 > DEFAULT_TEST_TIMEOUT / 3) {
      ld_warning(
          "Server process is taking a long time to start responding to the "
          "'info' command.  Dumping its error log to help debug issues:");
      int rv = dump_file_to_stderr(getLogPath().c_str());
      if (rv == 0) {
        should_dump_log = false;
      }
    }

    return false;
  };
  int rv =
      wait_until(("node " + std::to_string(node_index_) + " starts").c_str(),
                 started,
                 deadline);
  if (died) {
    rv = -1;
  }
  if (rv != 0) {
    ld_info("Node %d failed to start. Dumping its error log", node_index_);
    dump_file_to_stderr(getLogPath().c_str());
  } else {
    ld_info("Node %d started", node_index_);
  }
  return rv;
}

lsn_t Node::waitUntilAllShardsFullyAuthoritative(
    std::shared_ptr<Client> client) {
  std::vector<ShardID> shards;
  for (shard_index_t s = 0; s < num_db_shards_; ++s) {
    shards.push_back(ShardID(node_index_, s));
  }
  return IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, shards, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
}

lsn_t Node::waitUntilAllShardsAuthoritativeEmpty(
    std::shared_ptr<Client> client) {
  std::vector<ShardID> shards;
  for (shard_index_t s = 0; s < num_db_shards_; ++s) {
    shards.push_back(ShardID(node_index_, s));
  }
  return IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, shards, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
}

int Node::waitUntilKnownGossipState(
    node_index_t other_node_index,
    bool alive,
    std::chrono::steady_clock::time_point deadline) {
  const std::string key_expected =
      folly::to<std::string>("N", other_node_index);
  const std::string state_str = alive ? "ALIVE" : "DEAD";
  int rv = wait_until(("node " + std::to_string(node_index_) +
                       " learns through gossip that node " +
                       std::to_string(other_node_index) + " is " + state_str)
                          .c_str(),
                      [&]() { return gossipInfo()[key_expected] == state_str; },
                      deadline);
  if (rv == 0) {
    ld_info("Node %d transitioned to %s according to node %d",
            other_node_index,
            state_str.c_str(),
            node_index_);
  } else {
    ld_info(
        "Timed out waiting for node %d to see that node %d transitioned to %s",
        node_index_,
        other_node_index,
        state_str.c_str());
  }
  return rv;
}

int Node::waitUntilAvailable(std::chrono::steady_clock::time_point deadline) {
  return waitUntilKnownGossipState(node_index_, /* alive */ true, deadline);
}

void Node::waitUntilKnownDead(node_index_t other_node_index) {
  int rv = waitUntilKnownGossipState(other_node_index, /* alive */ false);
  ld_check(rv == 0);
}

int Node::waitForRecovery(logid_t log,
                          std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }

  // Wait for 'info sequencer' to output either:
  //  - last_released != LSN_INVALID, or
  //  - preempted_by.
  int rv = wait_until(
      ("node " + std::to_string(node_index_) + " finishes recovery of log " +
       std::to_string(log.val_))
          .c_str(),
      [&]() {
        auto seq = sequencerInfo(log);
        if (seq.empty()) {
          // There is no sequencer for this log on that node.
          return true;
        }

        if (seq["State"] == "PREEMPTED") {
          // If sequencer was preempted, consider recovery done.
          return true;
        }

        const std::string last_released = "Last released";
        if (seq[last_released] == "" || seq[last_released] == "0") {
          return false;
        }

        const std::string epoch = "Epoch";
        epoch_t seq_epoch = epoch_t(folly::to<epoch_t::raw_type>(seq[epoch]));
        epoch_t last_release_epoch =
            lsn_to_epoch(folly::to<lsn_t>(seq[last_released]));

        if (seq_epoch != last_release_epoch) {
          return false;
        }

        const std::string meta_last_released = "Meta last released";
        if (seq[meta_last_released] == "" || seq[meta_last_released] == "0") {
          return false;
        }

        return true;
      },
      deadline);

  if (rv == 0) {
    ld_info("Node %d finished recovery of log %lu", node_index_, log.val_);
  } else {
    ld_info("Timed out waiting for node %d to finish recovery of log %lu",
            node_index_,
            log.val_);
  }
  return rv;
}

std::unique_ptr<thrift::AdminAPIAsyncClient> Node::createAdminClient() {
  // This is a very bad way of handling Folly AsyncSocket flakiness in
  // combination with unix sockets.
  for (auto i = 0; i < 20; i++) {
    folly::SocketAddress address = getAdminAddress();
    auto transport =
        apache::thrift::async::TAsyncSocket::newSocket(&event_base_, address);
    auto channel = apache::thrift::HeaderClientChannel::newChannel(transport);
    channel->setTimeout(5000);
    if (channel->good()) {
      return std::make_unique<thrift::AdminAPIAsyncClient>(std::move(channel));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }
  ld_critical(
      "Couldn't create a thrift client for the Admin server for node %d",
      node_index_);
  return nullptr;
}

int Node::waitUntilNodeStateReady() {
  waitUntilAvailable();
  auto admin_client = createAdminClient();
  return wait_until(
      "LogDevice started but we are waiting for the EventLog to be replayed",
      [&]() {
        try {
          thrift::NodesStateRequest req;
          thrift::NodesStateResponse resp;
          admin_client->sync_getNodesState(resp, req);
          return true;
        } catch (thrift::NodeNotReady& e) {
          ld_info("getNodesState thrown NodeNotReady exception. Node %d is not "
                  "ready yet",
                  node_index_);
          return false;
        } catch (apache::thrift::transport::TTransportException& ex) {
          ld_info("AdminServer is not fully started yet, connections are "
                  "failing to node %d. ex: %s",
                  node_index_,
                  ex.what());
          return false;
        } catch (std::exception& ex) {
          ld_critical("An exception in AdminClient that we didn't expect: %s",
                      ex.what());
          return false;
        }
      });
}

int Node::waitUntilMaintenanceRSMReady() {
  waitUntilAvailable();
  auto admin_client = createAdminClient();
  return wait_until(
      "LogDevice started but we are waiting for the Maintenance RSM to be "
      "replayed",
      [&]() {
        try {
          thrift::MaintenancesFilter req;
          thrift::MaintenanceDefinitionResponse resp;
          admin_client->sync_getMaintenances(resp, req);
          return true;
        } catch (thrift::NodeNotReady& e) {
          ld_info(
              "getMaintenances thrown NodeNotReady exception. Node %d is not "
              "ready yet",
              node_index_);
          return false;
        } catch (apache::thrift::transport::TTransportException& ex) {
          ld_info("AdminServer is not fully started yet, connections are "
                  "failing to node %d. ex: %s",
                  node_index_,
                  ex.what());
          return false;
        } catch (std::exception& ex) {
          ld_critical("An exception in AdminClient that we didn't expect: %s",
                      ex.what());
          return false;
        }
      });
}

int Node::waitForPurge(logid_t log_id,
                       epoch_t epoch,
                       std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }

  ld_info("Waiting for node %d to finish purging of log %lu upto epoch %u.",
          node_index_,
          log_id.val_,
          epoch.val_);
  epoch_t new_lce(0);
  // Wait for 'logstoragestate' to output last_released with its epoch higher
  // or equal than @param epoch
  int rv =
      wait_until(("node " + std::to_string(node_index_) +
                  " finishes purging of log " + std::to_string(log_id.val_) +
                  " upto epoch " + std::to_string(epoch.val_))
                     .c_str(),
                 [&]() {
                   auto log_state = logState(log_id);
                   auto it = log_state.find("Last Released");
                   if (it == log_state.end()) {
                     return false;
                   }
                   const auto& lr_str = it->second;
                   if (lr_str.empty()) {
                     return false;
                   }
                   new_lce = lsn_to_epoch(folly::to<lsn_t>(lr_str));
                   return new_lce >= epoch;
                 },
                 deadline);

  if (rv == 0) {
    ld_info("Node %d finished purging of log %lu to epoch %u",
            node_index_,
            log_id.val_,
            new_lce.val_);
  } else {
    ld_error("Timed out waiting for node %d to finish purging of "
             "log %lu to epoch %u",
             node_index_,
             log_id.val_,
             epoch.val_);
  }
  return rv;
}

int Node::waitUntilLogsConfigSynced(
    lsn_t sync_lsn,
    std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }
  ld_info("Waiting for node %d to have read logs config log read up to %s.",
          node_index_,
          lsn_to_string(sync_lsn).c_str());

  const int rv =
      wait_until("logsconfig synced",
                 [&]() {
                   auto map = logsConfigInfo();
                   if (map.empty()) {
                     return false;
                   }
                   ld_check(map.count("Delta read ptr"));
                   std::string s = map["Delta read ptr"];
                   return !s.empty() && folly::to<lsn_t>(s) >= sync_lsn;
                 },
                 deadline);

  if (rv == 0) {
    ld_info("Node %d finished syncing logsconfig up to %s.",
            node_index_,
            lsn_to_string(sync_lsn).c_str());
  } else {
    ld_info("Timed out waiting for node %d to sync logsconfig up to %s.",
            node_index_,
            lsn_to_string(sync_lsn).c_str());
  }

  return rv;
}

int Node::waitUntilEventLogSynced(
    lsn_t sync_lsn,
    std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }

  ld_info("Waiting for node %d to have read event log upto %s.",
          node_index_,
          lsn_to_string(sync_lsn).c_str());

  const int rv = wait_until("event log is synced",
                            [&]() {
                              auto map = eventLogInfo();
                              if (map.empty()) {
                                return false;
                              }

                              // This is not a very nice usage of ld_check: it
                              // assumes something about behavior of a different
                              // process (logdeviced). The only excuse is that
                              // this is only used in tests at the moment.
                              ld_check(map.count("Propagated version"));

                              std::string s = map["Propagated version"];
                              ld_check(!s.empty());
                              return folly::to<lsn_t>(s) >= sync_lsn;
                            },
                            deadline);

  if (rv == 0) {
    ld_info("Node %d read event log upto %s.",
            node_index_,
            lsn_to_string(sync_lsn).c_str());
  } else {
    ld_info("Timed out waiting for node %d to read event log upto %s.",
            node_index_,
            lsn_to_string(sync_lsn).c_str());
  }

  return rv;
}

int Node::waitUntilExited() {
  ld_info("Waiting for node %d to exit", node_index_);
  auto res = logdeviced_->wait();
  ld_check(res.exited() || res.killed());
  if (res.killed()) {
    ld_warning("Node %d did not exit cleanly (signal %d)",
               node_index_,
               res.killSignal());
  } else {
    ld_info("Node %d exited cleanly", node_index_);
  }
  logdeviced_.reset();
  return res.killed() ? 128 + res.killSignal() : res.exitStatus();
}

void Node::suspend() {
  ld_info("Suspencing node %d", node_index_);

  // Make sure the node doesn't hold any file locks while stopped.
  std::string response = sendCommand("pause_file_epoch_store");
  if (response.substr(0, 2) != "OK") {
    ld_error("Failed to pause_file_epoch_store on N%d: %s",
             node_index_,
             sanitize_string(response).c_str());
  }

  stopped_ = true;
  this->signal(SIGSTOP);
  // SIGSTOP is not immediate.  Wait until the process has stopped.
  siginfo_t infop;
  int rv = waitid(P_PID, logdeviced_->pid(), &infop, WSTOPPED);
  if (rv != 0) {
    ld_warning("waitid(pid=%d) failed with errno %d (%s)",
               int(logdeviced_->pid()),
               errno,
               strerror(errno));
  }
  ld_info("Suspended node %d", node_index_);
}

void Node::resume() {
  ld_info("Resuming node %d", node_index_);
  this->signal(SIGCONT);
  siginfo_t infop;
  int rv = waitid(P_PID, logdeviced_->pid(), &infop, WCONTINUED);
  if (rv != 0) {
    ld_warning("waitid(pid=%d) failed with errno %d (%s)",
               int(logdeviced_->pid()),
               errno,
               strerror(errno));
  }
  stopped_ = false;

  // Allow the node to use flock() again.
  std::string response = sendCommand("unpause_file_epoch_store");
  if (response.substr(0, 2) != "OK") {
    ld_error("Failed to unpause_file_epoch_store on N%d: %s",
             node_index_,
             sanitize_string(response).c_str());
  }

  ld_info("Resumed node %d", node_index_);
}

std::unique_ptr<ShardedLocalLogStore> Node::createLocalLogStore() {
  RocksDBSettings rocks_settings = create_default_settings<RocksDBSettings>();
  rocks_settings.allow_fallocate = false;
  rocks_settings.auto_create_shards = true;
  rocks_settings.partitioned = rocksdb_type_ == RocksDBType::PARTITIONED;
  // Tell logsdb to not create partitions automatically, in particular to
  // not create lots of partitions if we write a record with very old timestamp.
  rocks_settings.partition_duration_ = std::chrono::seconds(0);

  return std::make_unique<ShardedRocksDBLocalLogStore>(
      getDatabasePath(),
      num_db_shards_,
      create_default_settings<Settings>(),
      UpdateableSettings<RocksDBSettings>(rocks_settings),
      UpdateableSettings<RebuildingSettings>(),
      nullptr,
      nullptr);
}

void Node::corruptShards(std::vector<uint32_t> shards,
                         std::unique_ptr<ShardedLocalLogStore> sharded_store) {
  if (!sharded_store) {
    sharded_store = createLocalLogStore();
  }
  // Collect paths to RocksDB instances
  std::vector<std::string> paths;
  for (auto idx : shards) {
    RocksDBLogStoreBase* store =
        dynamic_cast<RocksDBLogStoreBase*>(sharded_store->getByIndex(idx));
    ld_check(store != nullptr);
    paths.push_back(store->getDBPath());
  }
  sharded_store.reset(); // close DBs before corrupting them

  std::mt19937_64 rng(0xff00abcd);
  for (auto path : paths) {
    // Open all files in the RocksDB directory and overwrite them with
    // random data.  This should ensure that RocksDB fails to open the DB.
    using namespace boost::filesystem;
    for (recursive_directory_iterator it(path), end; it != end; ++it) {
      ld_check(!is_directory(*it));
      std::vector<char> junk(file_size(*it));
      std::generate(junk.begin(), junk.end(), rng);
      FILE* fp = fopen(it->path().c_str(), "w");
      ld_check(fp != nullptr);
      SCOPE_EXIT {
        fclose(fp);
      };
      int rv = fwrite(junk.data(), 1, junk.size(), fp);
      ld_check(rv == junk.size());
    }
  }
}

namespace {
// Helper classes and functions used to parse the output of admin commands
template <typename T>
struct format_traits;
template <>
struct format_traits<int64_t> {
  typedef int64_t value_type;
  static constexpr const char* fmt = "%ld";
};
template <>
struct format_traits<std::string> {
  typedef char value_type[101];
  static constexpr const char* fmt = "%100s";
};
template <typename T>
std::map<std::string, T> parse(std::string output, std::string prefix) {
  std::map<std::string, T> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    typename format_traits<T>::value_type value;
    std::string format = prefix + " %100s " + format_traits<T>::fmt;
    char name[101];
    if (sscanf(line.c_str(), format.c_str(), name, &value) == 2) {
      out[name] = value;
    }
  }
  return out;
}

/*
 * Parses a line like:
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns map entries like [N6]["ALIVE"], [N7]["SUSPECT"], ...
 */
std::map<std::string, std::string> parseGossipState(std::string output) {
  std::map<std::string, std::string> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    auto to = line.find_last_of(")");
    auto from = line.rfind(" ", to) + 1;
    std::string value = line.substr(from, to - from);

    std::string format = "GOSSIP %100s ";
    std::array<char, 101> name{};
    if (sscanf(line.c_str(), format.c_str(), name.begin()) == 1) {
      out[name.cbegin()] = value;
    }
  }

  return out;
}

/**
 * Parses a string like:
 *
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns map from node name to a pair of status and gossip: count.
 **/
std::map<std::string, std::pair<std::string, uint64_t>>
parseGossipCount(std::string output) {
  std::map<std::string, std::pair<std::string, uint64_t>> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);

  for (const std::string& line : lines) {
    std::array<char, 101> name = {{0}};
    std::array<char, 101> status = {{0}};
    uint64_t count;

    if (sscanf(line.c_str(),
               "GOSSIP %100s %100s (gossip: %lu",
               name.data(),
               status.data(),
               &count) == 3) {
      out[name.data()] = std::pair<std::string, uint64_t>(status.data(), count);
    }
  }

  return out;
}

/**
 * Parses a string like:
 *
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns a map with entries like [N6]["BOYCOTTED"], [N7]["-"]
 **/
std::map<std::string, std::string> parseGossipBoycottState(std::string output) {
  std::map<std::string, std::string> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    // read (max) 100 characters for name, then scan without assignment until a
    // close parenthesis, then read the boycott state
    std::string format = "GOSSIP %100s %*[^)]) %10s";
    std::array<char, 101> name{};
    std::array<char, 11> value{};
    if (sscanf(line.c_str(), format.c_str(), name.begin(), value.begin()) ==
        2) {
      out[name.cbegin()] = value.cbegin();
    }
  }

  return out;
}

/**
 * Parses the output of an admin command that outputs a json generated by the
 * AdminCommandTable utility (@see logdevice/common/AdminCommandTable.h) and
 * returns a vector of map from column name to value.
 */
std::vector<std::map<std::string, std::string>>
parseJsonAdminCommand(std::string data) {
  std::vector<std::map<std::string, std::string>> res;
  folly::dynamic map;
  try {
    map = folly::parseJson(data.substr(0, data.rfind("END")));
  } catch (std::runtime_error& e) {
    ld_error("Invalid json: %s", data.c_str());
    return {};
  }
  const auto headers = map["headers"];
  for (const auto& row : map["rows"]) {
    if (row.size() != headers.size()) {
      ld_error("Found row with invalid number of columns");
      ld_check(false);
      continue;
    }
    std::map<std::string, std::string> map_row;
    for (int i = 0; i < headers.size(); ++i) {
      const std::string v = row[i].isString() ? row[i].asString().c_str() : "";
      map_row[headers[i].asString().c_str()] = v;
      // map_row.insert(std::make_pair(headers[i], v));
    }
    res.emplace_back(std::move(map_row));
  }
  return res;
}
} // namespace

std::map<std::string, int64_t> Node::stats() const {
  return parse<int64_t>(sendCommand("stats2"), "STAT");
}

std::map<std::string, std::string> Node::logState(logid_t log_id) const {
  std::string command =
      "info log_storage_state --json --logid " + std::to_string(log_id.val_);
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    // There is no sequencer for this log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

/*
 * Sends inject shard fault command to the node:
 * "inject shard_fault "
 *      "<shard#|a|all> "
 *      "<d|data|m|metadata|a|all|n|none> "
 *      "<r|read|w|write|a|all|n|none> "
 *      "<io_error|corruption|latency|none> "
 *      "[--single_shot] "
 *      "[--chance=PERCENT]"
 *      "[--latency=LATENCY_MS]";
 */
bool Node::injectShardFault(std::string shard,
                            std::string data_type,
                            std::string io_type,
                            std::string code,
                            bool single_shot,
                            folly::Optional<double> chance,
                            folly::Optional<uint32_t> latency_ms) {
  std::string cmd =
      folly::format(
          "inject shard_fault {} {} {} {}", shard, data_type, io_type, code)
          .str();
  if (single_shot) {
    cmd += " --single_shot";
  }
  if (chance.hasValue()) {
    cmd += folly::format(" --chance={}", chance.value()).str();
  }
  if (latency_ms.hasValue()) {
    cmd += folly::format(" --latency={}", latency_ms.value()).str();
  }
  cmd += " --force"; // Within tests, it's fine to inject errors on opt builds.
  auto reply = sendCommand(cmd);
  ld_check(reply == "END\r\n");
  return true;
}

void Node::gossipBlacklist(node_index_t node_id) const {
  auto reply = sendCommand("gossip blacklist " + std::to_string(node_id));
  ld_check(reply ==
           "GOSSIP N" + std::to_string(node_id) + " BLACKLISTED\r\nEND\r\n");
}

void Node::gossipWhitelist(node_index_t node_id) const {
  auto reply = sendCommand("gossip whitelist " + std::to_string(node_id));
  ld_check(reply ==
           "GOSSIP N" + std::to_string(node_id) + " WHITELISTED\r\nEND\r\n");
}

void Node::newConnections(bool accept) const {
  auto reply = sendCommand(std::string("newconnections ") +
                           (accept ? "accept" : "reject"));
  ld_check(reply == "END\r\n");
}

void Node::startRecovery(logid_t logid) const {
  auto logid_string = std::to_string(logid.val_);
  auto reply = sendCommand("startrecovery " + logid_string);
  ld_check_eq(
      reply,
      folly::format("Started recovery for logid {}, result success\r\nEND\r\n",
                    logid_string)
          .str());
}

std::string Node::upDown(const logid_t logid) const {
  return sendCommand("up " + std::to_string(logid.val_));
}

std::map<std::string, std::string> Node::sequencerInfo(logid_t log_id) const {
  const std::string command =
      "info sequencers " + std::to_string(log_id.val_) + " --json";
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    // There is no sequencer for this log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::map<std::string, std::string> Node::eventLogInfo() const {
  const std::string command = "info event_log --json";
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    // This node does not seem to be reading the event log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::map<std::string, std::string> Node::logsConfigInfo() const {
  const std::string command = "info logsconfig_rsm --json";
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    // This node does not seem to be reading the event log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::vector<std::map<std::string, std::string>> Node::socketInfo() const {
  const std::string command = "info sockets --json";
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::vector<std::map<std::string, std::string>> Node::partitionsInfo() const {
  const std::string command = "info partitions --spew --json";
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::map<std::string, std::string> Node::gossipState() const {
  return parseGossipState(sendCommand("info gossip"));
}

std::map<std::string, std::pair<std::string, uint64_t>>
Node::gossipCount() const {
  return parseGossipCount(sendCommand("info gossip"));
}

std::map<std::string, std::string> Node::gossipInfo() const {
  return parse<std::string>(sendCommand("info gossip"), "GOSSIP");
}

std::map<std::string, bool> Node::gossipStarting() const {
  std::map<std::string, bool> out;
  auto cmd_result = sendCommand("info gossip --json");
  cmd_result = cmd_result.substr(0, cmd_result.rfind("END"));
  if (cmd_result == "") {
    return out;
  }

  auto obj = folly::parseJson(cmd_result);
  for (auto& state : obj["states"]) {
    int is_starting = state["detector"]["starting"].getInt();
    out[state["node_id"].getString()] =
        (state["status"].getString() == "ALIVE" && is_starting);
  }
  return out;
}

std::map<std::string, bool> Node::gossipBoycottState() const {
  auto string_state = parseGossipBoycottState(sendCommand("info gossip"));
  std::map<std::string, bool> bool_state;
  std::transform(string_state.cbegin(),
                 string_state.cend(),
                 std::inserter(bool_state, bool_state.begin()),
                 [](const auto& entry) {
                   return std::make_pair<std::string, bool>(
                       std::string{entry.first}, entry.second == "BOYCOTTED");
                 });
  return bool_state;
}

void Node::resetBoycott(node_index_t node_index) const {
  sendCommand("boycott_reset " + std::to_string(node_index));
}

std::map<std::string, std::string> Node::domainIsolationInfo() const {
  return parse<std::string>(sendCommand("info gossip"), "DOMAIN_ISOLATION");
}

std::vector<std::map<std::string, std::string>>
Node::partitionsInfo(shard_index_t shard, int level) const {
  const std::string command =
      folly::format("info partitions {} --json --level {}", shard, level).str();
  auto data = parseJsonAdminCommand(sendCommand(command));
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::map<shard_index_t, RebuildingRangesMetadata> Node::dirtyShardInfo() const {
  auto data =
      parseJsonAdminCommand(sendCommand("info shards --json --dirty-as-json"));
  ld_check(!data.empty());
  std::map<shard_index_t, RebuildingRangesMetadata> result;
  for (const auto& row : data) {
    const auto shard = row.find("Shard");
    const auto dirty_state = row.find("Dirty State");
    if (shard == row.end() || dirty_state == row.end() ||
        dirty_state->second.empty() || dirty_state->second == "{}" ||
        dirty_state->second == "UNKNOWN") {
      continue;
    }
    try {
      folly::dynamic obj = folly::parseJson(dirty_state->second);
      RebuildingRangesMetadata rrm;
      if (!RebuildingRangesMetadata::fromFollyDynamic(obj, rrm)) {
        ld_check(false);
        continue;
      }
      result.emplace(std::stoi(shard->second), rrm);
    } catch (...) {
      ld_check(false);
    }
  }
  return result;
}

partition_id_t Node::createPartition(uint32_t shard) {
  std::string out = sendCommand("logsdb create " + std::to_string(shard));
  const std::string expected = "Created partition ";
  if (out.substr(0, expected.size()) != expected) {
    ld_error(
        "Failed to create partition on N%d: %s", (int)node_index_, out.c_str());
    return PARTITION_INVALID;
  }
  return std::stol(out.substr(expected.size()));
}

int Node::compact(logid_t logid) const {
  std::string command_str = std::string("compact ") +
      (logid == LOGID_INVALID ? "--all" : std::to_string(logid.val_));
  std::string stdout = sendCommand(command_str);

  std::vector<std::string> lines;
  folly::split("\r\n", stdout, lines, /* ignoreEmpty */ true);

  std::string success_token("Successfully");
  if (!lines.empty() &&
      !lines[0].compare(0, success_token.size(), success_token)) {
    return 0;
  }
  return -1;
}

void Node::updateSetting(std::string name, std::string value) {
  sendCommand(folly::format("set {} {} --ttl max", name, value).str());
  // Assert that the setting was successfully changed
  auto data = parseJsonAdminCommand(sendCommand("info settings --json"));
  ld_check(!data.empty());
  for (int i = 1; i < data.size(); i++) {
    auto& row = data[i];
    if (row["Name"] == name) {
      if (row["Current Value"] != value) {
        // Maybe setting didn't update because something is broken.
        // Or maybe you set it to "1,2,3" but 'info settings'
        // printed it as "1, 2, 3".
        // Let's be conservative and crash.
        ld_critical(
            "Unexpected value in \"info settings\" on N%d after updating "
            "setting %s: expected %s, found %s. This is either a bug in "
            "settings or a benign formatting difference. If it's the latter "
            "please change your test to use canonical formatting.",
            (int)node_index_,
            name.c_str(),
            value.c_str(),
            row["Current Value"].c_str());
        std::abort();
      }
      return;
    }
  }
  ld_check(false);
}

void Node::unsetSetting(std::string name) {
  sendCommand("unset " + name);
  // Assert that the setting was successfully changed
  auto data = parseJsonAdminCommand(sendCommand("info settings --json"));
  ld_check(!data.empty());
  for (int i = 1; i < data.size(); i++) {
    auto& row = data[i];
    if (row["Name"] == name) {
      ld_check_eq(row["From Admin Cmd"], "");
      return;
    }
  }
  ld_check(false);
}

void Cluster::populateClientSettings(
    std::unique_ptr<ClientSettings>& settings) const {
  if (!settings) {
    settings.reset(ClientSettings::create());
  }

  int rv;
  // Instantiate StatsHolder in tests so that counters can be queried
  rv = settings->set("client-test-force-stats", "true");
  ld_check(rv == 0);
  // But disable publishing
  rv = settings->set("stats-collection-interval", "-1s");
  ld_check(rv == 0);
  // We don't need a ton of workers in the test client
  if (!settings->get("num-workers") ||
      settings->get("num-workers").value() == "cores") {
    rv = settings->set("num-workers", "5");
    ld_check(rv == 0);
  }
  if (!settings->isOverridden("node-stats-send-period")) {
    // Make sure node stats would be sent in most tests for better coverage
    rv = settings->set("node-stats-send-period", "100ms");
    ld_check(rv == 0);
  }
  if (!settings->isOverridden("ssl-ca-path") && !no_ssl_address_) {
    // Set CA cert path so the client can verify the server's identity.
    // Most clients will already have this settings set via the config, but
    // we have a couple of tests that load custom configs, so this ensures it's
    // set in those
    rv = settings->set(
        "ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"));
    ld_check(rv == 0);
  }
}

namespace {

class StaticSequencerLocatorFactory : public SequencerLocatorFactory {
  std::string identifier() const override {
    return "static";
  }

  std::string displayName() const override {
    return "Static sequencer placement";
  }
  std::unique_ptr<SequencerLocator>
  operator()(std::shared_ptr<UpdateableConfig> config) override {
    return std::make_unique<StaticSequencerLocator>(std::move(config));
  }
};

} // namespace

std::shared_ptr<Client>
Cluster::createClient(std::chrono::milliseconds timeout,
                      std::unique_ptr<ClientSettings> settings,
                      std::string credentials) {
  // Using the ClientImpl constructor directly so that we can have it share
  // our UpdateableConfig instance (instead of reading the file).  This way
  // in-process Client instances will see config updates instantly, making
  // tests behave more intuitively.

  populateClientSettings(settings);

  // Client should update its settings from the config file
  ClientSettingsImpl* impl_settings =
      static_cast<ClientSettingsImpl*>(settings.get());
  if (impl_settings != nullptr) {
    auto settings_updater = impl_settings->getSettingsUpdater();
    auto update_settings = [settings_updater](ServerConfig& config) -> bool {
      auto settings = config.getClientSettingsConfig();

      try {
        settings_updater->setFromConfig(settings);
      } catch (const boost::program_options::error&) {
        return false;
      }
      return true;
    };

    // do an initial read for the settings since the config is already loaded
    update_settings(*config_->getServerConfig());
    // subscribe for config updates, always update the settings
    server_config_hook_handles_.push_back(
        config_->updateableServerConfig()->addHook(std::move(update_settings)));
  }

  // original client settings loaded at the time of loading the config
  ClientSettingsImpl* original_settings =
      static_cast<ClientSettingsImpl*>(client_settings_.get());

  if (original_settings->getSettings()->enable_logsconfig_manager) {
    settings->set("enable-logsconfig-manager", "true");
  }

  PluginVector seed_plugins = getClientPluginProviders();
  if (!hash_based_sequencer_assignment_) {
    // assume N0 runs sequencers for all logs
    seed_plugins.emplace_back(
        std::make_unique<StaticSequencerLocatorFactory>());
  }
  std::shared_ptr<PluginRegistry> plugin_registry =
      std::make_shared<PluginRegistry>(std::move(seed_plugins));
  ld_info(
      "Plugins loaded: %s", plugin_registry->getStateDescriptionStr().c_str());

  return std::make_shared<ClientImpl>(cluster_name_,
                                      config_,
                                      credentials,
                                      "",
                                      timeout,
                                      std::move(settings),
                                      plugin_registry);
}

std::shared_ptr<Client> Cluster::createIndependentClient(
    std::chrono::milliseconds timeout,
    std::unique_ptr<ClientSettings> settings) const {
  populateClientSettings(settings);
  return ClientFactory()
      .setClusterName(cluster_name_)
      .setTimeout(timeout)
      .setClientSettings(std::move(settings))
      .create(config_path_);
}

namespace {
class IntegrationTestFileEpochStore : public FileEpochStore {
 public:
  explicit IntegrationTestFileEpochStore(
      std::string path,
      const std::shared_ptr<UpdateableNodesConfiguration>& config)
      : FileEpochStore(std::move(path), nullptr, config) {}

 protected:
  void postCompletionMetaData(
      EpochStore::CompletionMetaData cf,
      Status status,
      logid_t log_id,
      std::unique_ptr<EpochMetaData> metadata = nullptr,
      std::unique_ptr<EpochStoreMetaProperties> meta_props = nullptr) override {
    cf(status, log_id, std::move(metadata), std::move(meta_props));
  }
  void postCompletionLCE(EpochStore::CompletionLCE cf,
                         Status status,
                         logid_t log_id,
                         epoch_t epoch,
                         TailRecord tail_record) override {
    cf(status, log_id, epoch, tail_record);
  }
};
} // namespace

std::unique_ptr<EpochStore> Cluster::createEpochStore() {
  return std::make_unique<IntegrationTestFileEpochStore>(
      epoch_store_path_, getConfig()->updateableNodesConfiguration());
}

void Cluster::setStartingEpoch(logid_t log_id,
                               epoch_t epoch,
                               epoch_t last_expected_epoch) {
  auto epoch_store = createEpochStore();
  Semaphore semaphore;

  if (last_expected_epoch == EPOCH_INVALID) {
    // either expecting EPOCH_MIN + 1 or unprovisioned data, depending on the
    // test configuration
    epoch_store->readMetaData(log_id,
                              [&semaphore, &last_expected_epoch](
                                  Status status,
                                  logid_t /*log_id*/,
                                  std::unique_ptr<EpochMetaData> info,
                                  std::unique_ptr<EpochStoreMetaProperties>) {
                                if (status == E::OK) {
                                  ld_assert(info != nullptr);
                                  ld_assert_eq(
                                      EPOCH_MIN.val() + 1, info->h.epoch.val());
                                  last_expected_epoch = EPOCH_MIN;
                                } else {
                                  ld_assert_eq(E::NOTFOUND, status);
                                }
                                semaphore.post();
                              });
    semaphore.wait();
  }

  for (epoch_t e = epoch_t(last_expected_epoch.val_ + 1); e < epoch; ++e.val_) {
    epoch_store->createOrUpdateMetaData(
        log_id,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            getConfig()->get(),
            getConfig()->get()->getNodesConfigurationFromServerConfigSource()),
        [&semaphore, e](Status status,
                        logid_t,
                        std::unique_ptr<EpochMetaData> info,
                        std::unique_ptr<EpochStoreMetaProperties>) {
          ld_assert_eq(E::OK, status);
          ld_assert(info != nullptr);
          ld_assert_eq(e.val() + 1, info->h.epoch.val());
          semaphore.post();
        },
        MetaDataTracer());
    semaphore.wait();
  }
}

std::unique_ptr<MetaDataProvisioner> Cluster::createMetaDataProvisioner() {
  auto fn = [this](node_index_t nid) -> std::shared_ptr<ShardedLocalLogStore> {
    return getNode(nid).createLocalLogStore();
  };
  return std::make_unique<MetaDataProvisioner>(
      createEpochStore(), getConfig(), fn);
}

int Cluster::replace(node_index_t index, bool defer_start) {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);
  ld_debug("replacing node %d", (int)index);

  if (hasStorageRole(index)) {
    ld_check(getNodeReplacementCounter(index) ==
             config_->get()->serverConfig()->getNode(index)->generation);
  }

  for (int outer_try = 0, gen = getNodeReplacementCounter(index) + 1;
       outer_try < outer_tries_;
       ++outer_try, ++gen) {
    // Kill current node and erase its data
    nodes_.at(index).reset();

    Configuration::Nodes nodes = config_->get()->serverConfig()->getNodes();
    // addrs[0] - non-SSL address pair
    // addrs[1] - SSL address pair
    std::array<SockaddrPair, 2> addrs;

    for (int ssl = 0; ssl <= (no_ssl_address_ ? 0 : 1); ++ssl) {
      if (ssl) {
        ld_check(nodes.at(index).ssl_address);
      }
      Sockaddr& prev_address =
          ssl ? *nodes[index].ssl_address : nodes[index].address;
      // If the node was using a tcp port, we need to reserve new ports.
      if (!prev_address.isUnixAddress()) {
        std::vector<detail::PortOwnerPtrTuple> port_pairs;
        if (detail::find_free_port_set(1, port_pairs) != 0) {
          ld_error("Could not find two adjacent free ports to replace "
                   "LogDevice node");
          return -1;
        }
        addrs[ssl] = SockaddrPair::fromTcpPortPair(std::move(port_pairs[0]));
      } else {
        addrs[ssl] = SockaddrPair::buildUnixSocketPair(
            Cluster::getNodeDataPath(root_path_, index, gen), ssl);
      }
    }

    nodes[index].address = addrs[0].protocol_addr_;
    if (!no_ssl_address_) {
      nodes[index].ssl_address.assign(addrs[1].protocol_addr_);
    }

    if (hasStorageRole(index)) {
      // only bump the config generation if the node has storage role
      nodes[index].generation = gen;
    }
    // always bump the internal node replacement counter
    setNodeReplacementCounter(index, gen);

    Configuration::NodesConfig nodes_config(std::move(nodes));
    auto config = config_->get();
    std::shared_ptr<ServerConfig> new_server_config =
        config->serverConfig()->withNodes(std::move(nodes_config));

    // Update config on disk so that other nodes become aware of the swap as
    // soon as possible
    int rv = writeConfig(new_server_config.get(), config->logsConfig().get());
    if (rv != 0) {
      return -1;
    }
    // Wait for all nodes to pick up the config update
    waitForConfigUpdate();

    nodes_[index] = createNode(index, std::move(addrs[0]), std::move(addrs[1]));
    if (defer_start) {
      return 0;
    }
    if (start({index}) == 0) {
      return 0;
    }
  }

  ld_error("Failed to replace");
  return -1;
}

int Cluster::bumpGeneration(node_index_t index) {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);
  Configuration::Nodes nodes = config_->get()->serverConfig()->getNodes();
  if (hasStorageRole(index)) {
    // expect internal tracked replacemnt counter is in sync with configuration
    ld_check(getNodeReplacementCounter(index) == nodes.at(index).generation);
    // only bump configuration generation if the node has storage role
    ++nodes.at(index).generation;
  }

  // always bump the internal node replacement counter
  bumpNodeReplacementCounter(index);

  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto config = config_->get();
  std::shared_ptr<ServerConfig> new_server_config =
      config->serverConfig()
          ->withNodes(std::move(nodes_config))
          ->withIncrementedVersion();
  int rv = writeConfig(new_server_config.get(), config->logsConfig().get());
  if (rv != 0) {
    return -1;
  }
  return 0;
}

int Cluster::updateNodeAttributes(node_index_t index,
                                  configuration::StorageState storage_state,
                                  int sequencer_weight,
                                  folly::Optional<bool> enable_sequencing) {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);
  Configuration::Nodes nodes = config_->get()->serverConfig()->getNodes();
  if (!nodes.count(index)) {
    ld_error("No such node: %d", (int)index);
    ld_check(false);
    return -1;
  }
  const auto& node = nodes[index];
  if (node.storage_attributes != nullptr) {
    node.storage_attributes->state = storage_state;
    if (storage_state == configuration::StorageState::READ_WRITE &&
        node.storage_attributes->capacity == 0) {
      node.storage_attributes->capacity = 1;
    }
  }

  if (node.sequencer_attributes != nullptr) {
    node.sequencer_attributes->setWeight(sequencer_weight);
    if (enable_sequencing.hasValue()) {
      node.sequencer_attributes->setEnabled(enable_sequencing.value());
    }
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));
  auto config = config_->get();
  std::shared_ptr<ServerConfig> new_server_config =
      config->serverConfig()
          ->withNodes(std::move(nodes_config))
          ->withIncrementedVersion();
  int rv = writeConfig(new_server_config.get(), config->logsConfig().get());
  if (rv != 0) {
    return -1;
  }
  return 0;
}

void Cluster::waitForConfigUpdate() {
  // TODO: make it work with one config per nodes.
  ld_check(!one_config_per_node_);
  auto check = [this]() {
    std::shared_ptr<const Configuration> our_config = config_->get();
    const std::string expected_text = our_config->toString() + "\r\nEND\r\n";
    for (auto& it : nodes_) {
      if (it.second && it.second->logdeviced_ && !it.second->stopped_) {
        std::string node_text = it.second->sendCommand("info config");
        if (node_text != expected_text) {
          ld_info("Waiting for N%d:%d to pick up the most recent config",
                  it.first,
                  our_config->serverConfig()->getNode(it.first)->generation);
          return false;
        }
      }
    }
    return true;
  };
  wait_until("config update", check);
}

int Cluster::waitForRecovery(std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<const Configuration> config = config_->get();
  const auto& logs = config->localLogsConfig();
  ld_debug("Waiting for recovery of %lu data logs.", logs->size());

  for (auto& it : nodes_) {
    node_index_t idx = it.first;
    if (!config->serverConfig()->getNode(idx)->isSequencingEnabled()) {
      continue;
    }

    for (auto it = logs->logsBegin(); it != logs->logsEnd(); ++it) {
      logid_t log(it->first);
      int rv = getNode(idx).waitForRecovery(log, deadline);

      if (rv) {
        return -1;
      }
    }
  }

  return 0;
}

int Cluster::waitUntilAllAvailable(
    std::chrono::steady_clock::time_point deadline) {
  int rv = 0;
  for (auto& it : getNodes()) {
    node_index_t idx = it.first;
    rv |= getNode(idx).waitUntilAvailable(deadline);
  }
  return rv;
}

int Cluster::waitUntilLogsConfigSynced(
    lsn_t sync_lsn,
    const std::vector<node_index_t>& nodes,
    std::chrono::steady_clock::time_point deadline) {
  for (int n : nodes) {
    int rv = getNode(n).waitUntilLogsConfigSynced(sync_lsn, deadline);
    if (rv != 0) {
      return -1;
    }
  }

  return 0;
}

int Cluster::waitUntilEventLogSynced(
    lsn_t sync_lsn,
    const std::vector<node_index_t>& nodes,
    std::chrono::steady_clock::time_point deadline) {
  for (int n : nodes) {
    int rv = getNode(n).waitUntilEventLogSynced(sync_lsn, deadline);
    if (rv != 0) {
      return -1;
    }
  }

  return 0;
}

int Cluster::waitForMetaDataLogWrites(
    std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<const Configuration> config = config_->get();
  const auto& logs = config->localLogsConfig();

  auto check = [&]() {
    const std::string metadata_log_written = "Metadata log written";
    for (auto log_it = logs->logsBegin(); log_it != logs->logsEnd(); ++log_it) {
      logid_t log(log_it->first);
      epoch_t last_epoch{LSN_INVALID};
      epoch_t last_written_epoch{LSN_INVALID};
      for (auto& kv : nodes_) {
        node_index_t idx = kv.first;
        if (!config->serverConfig()->getNode(idx)->isSequencingEnabled()) {
          continue;
        }
        auto seq = kv.second->sequencerInfo(log);
        epoch_t epoch = epoch_t(atoi(seq["Epoch"].c_str()));
        last_epoch = std::max(last_epoch, epoch);
        if (seq[metadata_log_written] == "1") {
          last_written_epoch = std::max(last_written_epoch, epoch);
        }
      }
      if (last_epoch > last_written_epoch) {
        // The last activated sequencer has unwritten metadata
        return false;
      }
    }
    return true;
  };

  std::string msg = "metadata log records of " +
      folly::to<std::string>(logs->getLogMap().size()) +
      " data logs are written.";
  return wait_until(msg.c_str(), check, deadline);
}

int Cluster::waitUntilGossip(bool alive,
                             uint64_t targetNode,
                             std::set<uint64_t> nodesToSkip,
                             std::chrono::steady_clock::time_point deadline) {
  for (const auto& it : getNodes()) {
    if ((!alive && it.first == targetNode) ||
        nodesToSkip.count(it.first) != 0 || it.second->stopped_) {
      continue;
    }
    int rv = it.second->waitUntilKnownGossipState(targetNode, alive, deadline);
    if (rv != 0) {
      return rv;
    }
  }
  return 0;
}

int Cluster::checkConsistency(argv_t additional_args) {
  folly::Subprocess::Options options;
  options.parentDeathSignal(SIGKILL); // kill children if test process dies

  std::string checker_path = findBinary(CHECKER_PATH);
  if (checker_path.empty()) {
    return -1;
  }

  argv_t argv = {
      checker_path,
      "--config-path",
      config_path_,
      "--loglevel",
      dbg::loglevelToString(dbg::currentLevel),
      "--report-errors",
      "all",
  };

  argv.insert(argv.end(), additional_args.begin(), additional_args.end());

  auto proc = std::make_unique<folly::Subprocess>(argv, options);
  auto status = proc->wait();
  if (!status.exited()) {
    ld_error("checker did not exit properly: %s", status.str().c_str());
    return -1;
  }

  if (status.exitStatus() == 0) {
    return 0;
  } else {
    ld_error("checker exited with error %i", status.exitStatus());
    return -1;
  }
}

Cluster::~Cluster() {
  nodes_.clear();
  config_.reset();

  if (getenv_switch("LOGDEVICE_TEST_LEAVE_DATA")) {
    ld_info("LOGDEVICE_TEST_LEAVE_DATA environment variable was set.  Leaving "
            "data in: %s",
            root_path_.c_str());
  }
}

std::string ClusterFactory::actualServerBinary() const {
  const char* envpath = getenv("LOGDEVICE_TEST_BINARY");
  if (envpath != nullptr) {
    return envpath;
  }
  std::string relative_to_build_out =
      server_binary_.value_or(defaultLogdevicedPath());
  return findBinary(relative_to_build_out);
}

void ClusterFactory::setInternalLogAttributes(const std::string& name,
                                              logsconfig::LogAttributes attrs) {
  auto log_group_node = internal_logs_.insert(name, attrs);
  ld_check(log_group_node);
}

static void noop_signal_handler(int) {}

void maybe_pause_for_gdb(Cluster& cluster,
                         const std::vector<node_index_t>& indices) {
  if (!getenv_switch("LOGDEVICE_TEST_PAUSE_FOR_GDB")) {
    return;
  }

  fprintf(stderr,
          "\nLOGDEVICE_TEST_PAUSE_FOR_GDB environment variable was set.  "
          "Pausing to allow human to debug the system.\n\n");
  fprintf(stderr, "Attach GDB to server processes with:\n");
  for (int i : indices) {
    fprintf(stderr,
            "  Node N%d:%d: gdb %s %d\n",
            i,
            cluster.getConfig()->get()->serverConfig()->getNode(i)->generation,
            cluster.getNode(i).server_binary_.c_str(),
            cluster.getNode(i).logdeviced_->pid());
  }

  unsigned alarm_saved = alarm(0);

  fprintf(stderr, "\nResume this process with:\n");
  fprintf(stderr, "  kill -usr2 %d\n\n", getpid());
  struct sigaction act, oldact;
  act.sa_handler = noop_signal_handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGUSR2, &act, &oldact);
  pause();
  sigaction(SIGUSR2, &oldact, nullptr);

  alarm(alarm_saved);
}

static lsn_t writeToEventlog(Client& client, EventLogRecord& event) {
  logid_t event_log_id = configuration::InternalLogs::EVENT_LOG_DELTAS;

  ld_info("Writing to event log: %s", event.describe().c_str());

  // Retry for at most 30s to avoid test failures due to transient failures
  // writing to the event log.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};

  const int size = event.toPayload(nullptr, 0);
  ld_check(size > 0);
  void* buf = malloc(size);
  ld_check(buf);
  SCOPE_EXIT {
    free(buf);
  };
  int rv = event.toPayload(buf, size);
  ld_check(rv == size);
  Payload payload(buf, size);

  lsn_t lsn = LSN_INVALID;
  auto clientImpl = dynamic_cast<ClientImpl*>(&client);
  clientImpl->allowWriteInternalLog();
  rv = wait_until("writes to the event log succeed",
                  [&]() {
                    lsn = clientImpl->appendSync(event_log_id, payload);
                    return lsn != LSN_INVALID;
                  },
                  deadline);

  if (rv != 0) {
    ld_check(lsn == LSN_INVALID);
    ld_error("Could not write record %s in event log(%lu): %s(%s)",
             event.describe().c_str(),
             event_log_id.val_,
             error_name(err),
             error_description(err));
    return false;
  }

  ld_info("Wrote event log record with lsn %s", lsn_to_string(lsn).c_str());
  return lsn;
}

Status getSeqState(Client* client,
                   logid_t log_id,
                   SequencerState& seq_state,
                   bool wait_for_recovery) {
  Status st = E::OK;
  bool callback_called = false;
  auto callback = [&](GetSeqStateRequest::Result res) {
    st = res.status;
    seq_state.node = res.last_seq;
    seq_state.last_released_lsn = res.last_released_lsn;
    seq_state.next_lsn = res.next_lsn;
    callback_called = true;
  };

  GetSeqStateRequest::Options opts;
  GetSeqStateRequest::Context ctx{GetSeqStateRequest::Context::UNKNOWN};
  opts.wait_for_recovery = wait_for_recovery;
  opts.on_complete = callback;

  auto& processor = static_cast<ClientImpl*>(client)->getProcessor();
  std::unique_ptr<Request> req =
      std::make_unique<GetSeqStateRequest>(logid_t(log_id), ctx, opts);
  processor.blockingRequest(req);

  ld_check(callback_called);
  return st;
}

lsn_t requestShardRebuilding(Client& client,
                             node_index_t node,
                             uint32_t shard,
                             SHARD_NEEDS_REBUILD_flags_t flags,
                             RebuildingRangesMetadata* rrm) {
  SHARD_NEEDS_REBUILD_Header hdr(
      node, shard, "unittest", "IntegrationTestUtils", flags);
  SHARD_NEEDS_REBUILD_Event event(hdr, rrm);
  return writeToEventlog(client, event);
}

lsn_t markShardUndrained(Client& client, node_index_t node, uint32_t shard) {
  SHARD_UNDRAIN_Event event(node, shard);
  return writeToEventlog(client, event);
}

lsn_t markShardUnrecoverable(Client& client,
                             node_index_t node,
                             uint32_t shard) {
  SHARD_UNRECOVERABLE_Event event(node, shard);
  return writeToEventlog(client, event);
}

lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       std::set<AuthoritativeStatus> st,
                                       bool wait_for_rebuilding) {
  std::string reason = "shards ";
  for (int i = 0; i < shards.size(); ++i) {
    if (i > 0) {
      reason += ",";
    }
    reason += shards[i].toString();
  }
  reason += " to have their authoritative status changed to ";
  if (st.size() > 1) {
    reason += "{";
  }
  for (auto it = st.begin(); it != st.end(); ++it) {
    if (it != st.begin()) {
      reason += ", ";
    }
    reason += toString(*it);
  }
  if (st.size() > 1) {
    reason += "}";
  }

  ld_info("Waiting for %s", reason.c_str());
  auto start_time = SteadyTimestamp::now();

  lsn_t last_update = LSN_INVALID;

  int rv = EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        for (const ShardID& shard : shards) {
          std::vector<node_index_t> donors_remaining;
          const auto status = set.getShardAuthoritativeStatus(
              shard.node(), shard.shard(), donors_remaining);
          if (!st.count(status) ||
              (wait_for_rebuilding && !donors_remaining.empty())) {
            return true;
          }
        }
        last_update = set.getLastUpdate();
        return false;
      });

  auto seconds_waited =
      std::chrono::duration_cast<std::chrono::duration<double>>(
          SteadyTimestamp::now() - start_time);
  ld_info("Finished waiting for %s (%.3fs)",
          reason.c_str(),
          seconds_waited.count());

  ld_check(rv == 0);
  return last_update;
}

lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       AuthoritativeStatus st,
                                       bool wait_for_rebuilding) {
  return waitUntilShardsHaveEventLogState(
      client, shards, std::set<AuthoritativeStatus>({st}), wait_for_rebuilding);
}

lsn_t waitUntilShardHasEventLogState(std::shared_ptr<Client> client,
                                     ShardID shard,
                                     AuthoritativeStatus st,
                                     bool wait_for_rebuilding) {
  return waitUntilShardsHaveEventLogState(
      client, {shard}, st, wait_for_rebuilding);
}

int Cluster::getShardAuthoritativeStatusMap(ShardAuthoritativeStatusMap& map) {
  auto client = createClient();
  return EventLogUtils::getShardAuthoritativeStatusMap(*client, map);
}

int Cluster::shutdownNodes(const std::vector<node_index_t>& nodes) {
  std::vector<node_index_t> to_wait;
  for (auto i : nodes) {
    auto& n = getNode(i);
    if (n.isRunning()) {
      n.sendCommand("stop");
      to_wait.push_back(i);
    }
  }
  int res = 0;
  for (auto i : to_wait) {
    int rv = getNode(i).waitUntilExited();
    if (rv != 0) {
      res = -1;
    }
  }
  return res;
}

int Cluster::getHashAssignedSequencerNodeId(logid_t log_id, Client* client) {
  SequencerState seq_state;
  Status s = getSeqState(client, log_id, seq_state, true);
  return s == E::OK ? seq_state.node.index() : -1;
}

std::string findBinary(const std::string& relative_path) {
#ifdef FB_BUILD_PATHS
  // Inside FB ask the build system for the full path.  Note that we don't use
  // the plugin facility for this because we run most tests with minimal
  // dependencies (default plugin) to speed up build and run times.
  return facebook::files::FbcodePaths::findPathInFbcodeBin(relative_path);
#else
  return findFile(relative_path, /* require_excutable */ true);
#endif
}

bool Cluster::hasStorageRole(node_index_t node) const {
  const Configuration::Nodes& nodes =
      config_->get()->serverConfig()->getNodes();
  return nodes.at(node).hasRole(configuration::NodeRole::STORAGE);
}

int Cluster::writeConfig(const ServerConfig* server_cfg,
                         const LogsConfig* logs_cfg,
                         bool wait_for_update) {
  int rv = overwriteConfig(config_path_.c_str(),
                           server_cfg,
                           logs_cfg,
                           write_logs_config_file_separately_);
  if (rv != 0) {
    return rv;
  }
  if (sync_server_config_to_nodes_configuration_ && server_cfg != nullptr) {
    rv = updateNodesConfigurationFromServerConfig(server_cfg);
    if (rv != 0) {
      return rv;
    }
  }
  if (!wait_for_update) {
    return 0;
  }
  config_source_->thread()->advisePollingIteration();
  ld_check(write_logs_config_file_separately_ || server_cfg != nullptr);
  std::string expected_text =
      (server_cfg ? server_cfg : config_->get()->serverConfig().get())
          ->toString(logs_cfg);
  wait_until("Config update picked up",
             [&] { return config_->get()->toString() == expected_text; });
  return 0;
}

int Cluster::writeConfig(const Configuration& cfg, bool wait_for_update) {
  return writeConfig(
      cfg.serverConfig().get(), cfg.logsConfig().get(), wait_for_update);
}

int dump_file_to_stderr(const char* path) {
  FILE* fp = std::fopen(path, "r");
  if (fp == nullptr) {
    ld_error("fopen(\"%s\") failed with errno %d (%s)",
             path,
             errno,
             strerror(errno));
    return -1;
  }
  fprintf(stderr, "=== begin %s\n", path);
  std::vector<char> buf(16 * 1024);
  while (std::fgets(buf.data(), buf.size(), fp) != nullptr) {
    fprintf(stderr, "    %s", buf.data());
  }
  fprintf(stderr, "=== end %s\n", path);
  std::fclose(fp);
  return 0;
}
}}} // namespace facebook::logdevice::IntegrationTestUtils
