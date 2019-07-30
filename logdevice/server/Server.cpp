/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/Server.h"

#include "logdevice/admin/SimpleAdminServer.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/NodesConfigurationInit.h"
#include "logdevice/common/NodesConfigurationPublisher.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/StaticSequencerPlacement.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/plugin/AdminServerFactory.h"
#include "logdevice/common/plugin/BuiltinZookeeperClientFactory.h"
#include "logdevice/common/plugin/TraceLoggerFactory.h"
#include "logdevice/common/settings/SSLSettingValidation.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/LazySequencerPlacement.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/MyNodeIDFinder.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/UnreleasedRecordDetector.h"
#include "logdevice/server/ZookeeperEpochStore.h"
#include "logdevice/server/fatalsignal.h"
#include "logdevice/server/locallogstore/ClusterMarkerChecker.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/RecordCacheRepopulationTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/util.h"

using facebook::logdevice::configuration::LocalLogsConfig;

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;

static StatsHolder* errorStats = nullptr;

static void bumpErrorCounter(dbg::Level level) {
  switch (level) {
    case dbg::Level::INFO:
    case dbg::Level::NOTIFY:
    case dbg::Level::WARNING:
      STAT_INCR(errorStats, production_notices);
      return;
    case dbg::Level::ERROR:
      STAT_INCR(errorStats, severe_errors);
      return;
    case dbg::Level::CRITICAL:
    case dbg::Level::NONE:
      STAT_INCR(errorStats, critical_errors);
      return;
    case dbg::Level::SPEW:
    case dbg::Level::DEBUG:
      // Don't bother updating.
      return;
  }
  ld_check(false);
}

bool ServerParameters::shutdownIfMyNodeIdChanged(
    const NodesConfiguration& config) {
  if (!my_node_id_.has_value()) {
    return true;
  }
  if (!isSameMyNodeID(config)) {
    // If some other node took over our NodeID let's shutdown gracefully.
    // TODO: Assert same service discovery attributes as well
    ld_critical("MyNodeID is not the same anymore. Rejecting config.");
    if (server_settings_->shutdown_on_my_node_id_mismatch) {
      ld_critical("--shutdown-on-my-node-id-mismatch is set, gracefully "
                  "shutting down.");
      requestStop();
    }
    return false;
  }
  return true;
}

bool ServerParameters::isSameMyNodeID(const NodesConfiguration& config) {
  ld_check(my_node_id_.hasValue());

  ld_check(my_node_id_finder_);

  auto node_id = my_node_id_finder_->calculate(config);
  if (!node_id.hasValue()) {
    ld_error("Couldn't find my node index in config.");
    return false;
  }
  if (my_node_id_.value() != node_id.value()) {
    ld_error("My node ID changed from %s to %s.",
             my_node_id_->toString().c_str(),
             node_id->toString().c_str());

    return false;
  }
  return true;
}

bool ServerParameters::updateServerOrigin(ServerConfig& config) {
  if (!my_node_id_.has_value()) {
    return true;
  }

  // If the config doesn't have a valid ServerOrigin, then we are the source.
  if (!config.getServerOrigin().isNodeID()) {
    // Update the server origin of the config to my node ID if it is not
    // set already
    config.setServerOrigin(my_node_id_.value());
  }
  return true;
}

bool ServerParameters::updateConfigSettings(ServerConfig& config) {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Updating settings from config took %lums",
            msec_since(start_ts.timePoint()));
  };

  try {
    settings_updater_->setFromConfig(config.getServerSettingsConfig());
  } catch (const boost::program_options::error&) {
    return false;
  }
  return true;
}

bool ServerParameters::onServerConfigUpdate(ServerConfig& config) {
  return updateServerOrigin(config) && updateConfigSettings(config);
}

bool ServerParameters::setConnectionLimits() {
  if (server_settings_->fd_limit == 0 ||
      server_settings_->num_reserved_fds == 0) {
    ld_debug("not enforcing limits on incoming connections");
    return true;
  }

  std::shared_ptr<const Settings> settings = processor_settings_.get();
  const size_t nodes =
      updateable_config_->getNodesConfiguration()->clusterSize();
  const size_t workers = settings->num_workers;

  const int available =
      server_settings_->fd_limit - server_settings_->num_reserved_fds;
  if (available < 0) {
    ld_error("number of reserved fds (%d) is higher than the fd limit (%d)",
             server_settings_->num_reserved_fds,
             server_settings_->fd_limit);
    return false;
  }

  // To get the maximum number of connections the server's willing to accept,
  // subtract the expected number of outgoing connections -- one per worker
  // to each other node -- from the number of available fds (and some more,
  // to be on the safe side).
  int max_incoming = available - nodes * workers * 1.5;
  if (max_incoming < 1) {
    ld_error("not enough fds for incoming connections with fd limit %d and "
             "num reserved %d",
             server_settings_->fd_limit,
             server_settings_->num_reserved_fds);
    return false;
  }

  // In addition to outgoing connections, each node is expected to have one
  // connection from each of other nodes' worker threads, so take that into
  // account when calculating the max number of _client_ connections.
  int max_external = max_incoming - nodes * workers * 1.5;
  if (max_external < 1) {
    ld_error("not enough fds for external connections with fd limit %d and "
             "num reserved %d",
             server_settings_->fd_limit,
             server_settings_->num_reserved_fds);
    return false;
  }

  ld_info("Max incoming connections: %d", max_incoming);
  settings_updater_->setInternalSetting(
      "max-incoming-connections", std::to_string(max_incoming));

  ld_info("Max external connections: %d", max_external);
  settings_updater_->setInternalSetting(
      "max-external-connections", std::to_string(max_external));

  // We're not subscribing to config changes here because these require
  // restarting the server to take effect.
  STAT_SET(getStats(), fd_limit, server_settings_->fd_limit);
  STAT_SET(getStats(), num_reserved_fds, server_settings_->num_reserved_fds);
  STAT_SET(getStats(), max_incoming_connections, max_incoming);
  STAT_SET(getStats(), max_external_connections, max_external);

  return true;
}

bool ServerParameters::initMyNodeIDFinder() {
  std::unique_ptr<NodeIDMatcher> id_matcher;
  // TODO(T44427489): When name is enforced in config, we can always use the
  // name to search for ourself in the config.
  if (!server_settings_->unix_socket.empty()) {
    id_matcher = NodeIDMatcher::byUnixSocket(server_settings_->unix_socket);
  } else {
    id_matcher = NodeIDMatcher::byTCPPort(server_settings_->port);
  }

  if (id_matcher == nullptr) {
    return false;
  }

  my_node_id_finder_ = std::make_unique<MyNodeIDFinder>(std::move(id_matcher));
  return true;
}

ServerParameters::ServerParameters(
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    UpdateableSettings<LocalLogStoreSettings> locallogstore_settings,
    UpdateableSettings<GossipSettings> gossip_settings,
    UpdateableSettings<Settings> processor_settings,
    UpdateableSettings<RocksDBSettings> rocksdb_settings,
    UpdateableSettings<AdminServerSettings> admin_server_settings,
    std::shared_ptr<PluginRegistry> plugin_registry,
    std::function<void()> stop_handler)
    : plugin_registry_(std::move(plugin_registry)),
      server_stats_(StatsParams().setIsServer(true)),
      settings_updater_(std::move(settings_updater)),
      server_settings_(std::move(server_settings)),
      rebuilding_settings_(std::move(rebuilding_settings)),
      locallogstore_settings_(std::move(locallogstore_settings)),
      gossip_settings_(std::move(gossip_settings)),
      processor_settings_(std::move(processor_settings)),
      rocksdb_settings_(std::move(rocksdb_settings)),
      admin_server_settings_(std::move(admin_server_settings)),
      stop_handler_(std::move(stop_handler)) {
  ld_check(stop_handler_);

  // Note: this won't work well if there are multiple Server instances in the
  // same process: only one of them will get its error counter bumped. Also,
  // there's a data race if the following two lines are run from multiple
  // threads.
  bool multiple_servers_in_same_process =
      errorStats != nullptr || dbg::bumpErrorCounterFn != nullptr;
  errorStats = &server_stats_;
  dbg::bumpErrorCounterFn = &bumpErrorCounter;
  if (multiple_servers_in_same_process) {
    ld_warning("Multiple Server instances coexist in the same process. Only "
               "one of them will receive error stats ('severe_errors', "
               "'critical_errors' etc).");
  }

  auto updateable_server_config = std::make_shared<UpdateableServerConfig>();
  auto updateable_logs_config = std::make_shared<UpdateableLogsConfig>();
  auto updateable_zookeeper_config =
      std::make_shared<UpdateableZookeeperConfig>();
  updateable_config_ =
      std::make_shared<UpdateableConfig>(updateable_server_config,
                                         updateable_logs_config,
                                         updateable_zookeeper_config);

  server_config_hook_handles_.push_back(updateable_server_config->addHook(
      std::bind(&ServerParameters::onServerConfigUpdate,
                this,
                std::placeholders::_1)));
  nodes_configuration_hook_handles_.push_back(
      updateable_config_->updateableNodesConfiguration()->addHook(
          std::bind(&ServerParameters::shutdownIfMyNodeIdChanged,
                    this,
                    std::placeholders::_1)));

  {
    ConfigInit config_init(
        processor_settings_->initial_config_load_timeout, getStats());
    int rv = config_init.attach(server_settings_->config_path,
                                plugin_registry_,
                                updateable_config_,
                                nullptr,
                                processor_settings_);
    if (rv != 0) {
      throw ConstructorFailed();
    }
  }

  if (processor_settings_->enable_nodes_configuration_manager) {
    if (!initNodesConfiguration()) {
      throw ConstructorFailed();
    }
    ld_check(updateable_config_->getNodesConfigurationFromNCMSource() !=
             nullptr);
  }

  // Publish the NodesConfiguration for the first time and subscribe to
  // updates in the mean time, until later when the long-lived subscribing
  // NodesConfigurationPublisher gets created in the Processor.
  // TODO(T43023435): use an actual TraceLogger to log this initial update.
  NodesConfigurationPublisher publisher(
      updateable_config_,
      processor_settings_,
      std::make_shared<NoopTraceLogger>(updateable_config_));
  ld_check(updateable_config_->getNodesConfiguration() != nullptr);

  // Initialize the MyNodeIDFinder that will be used to find our NodeID from the
  // config.
  if (!initMyNodeIDFinder()) {
    ld_error("Failed to construct MyNodeIDFinder");
    throw ConstructorFailed();
  }

  {
    // Find our NodeID from the published NodesConfiguration
    ld_check(my_node_id_finder_);
    const auto& nodes_configuration =
        updateable_config_->getNodesConfiguration();
    auto my_id = my_node_id_finder_->calculate(*nodes_configuration);
    if (!my_id.hasValue()) {
      ld_error("Failed to identify my node index in config");
      throw ConstructorFailed();
    }
    ld_check(my_id->isNodeID());
    my_node_id_ = std::move(my_id);
  }

  if (updateable_logs_config->get() == nullptr) {
    // Initialize logdevice with an empty LogsConfig that only contains the
    // internal logs and is marked as not fully loaded.
    auto logs_config = std::make_shared<LocalLogsConfig>();
    updateable_config_->updateableLogsConfig()->update(logs_config);
  }

  auto config = updateable_config_->get();
  // sets the InternalLogs of LocalLogsConfig.
  config->localLogsConfig()->setInternalLogsConfig(
      config->serverConfig()->getInternalLogsConfig());

  NodeID node_id = my_node_id_.value();
  ld_info("My NodeID is %s", node_id.toString().c_str());
  const auto& nodes_configuration = updateable_config_->getNodesConfiguration();
  ld_check(
      nodes_configuration->isNodeInServiceDiscoveryConfig(node_id.index()));

  if (!setConnectionLimits()) {
    throw ConstructorFailed();
  }

  // Construct the Server Trace Logger
  std::shared_ptr<TraceLoggerFactory> trace_logger_factory =
      plugin_registry_->getSinglePlugin<TraceLoggerFactory>(
          PluginType::TRACE_LOGGER_FACTORY);
  if (!trace_logger_factory || processor_settings_->trace_logger_disabled) {
    trace_logger_ =
        std::make_shared<NoopTraceLogger>(updateable_config_, my_node_id_);
  } else {
    trace_logger_ = (*trace_logger_factory)(updateable_config_, my_node_id_);
  }

  storage_node_ = nodes_configuration->isStorageNode(my_node_id_->index());
  num_db_shards_ = storage_node_
      ? nodes_configuration->getNodeStorageAttribute(my_node_id_->index())
            ->num_shards
      : 0;

  run_sequencers_ = nodes_configuration->isSequencerNode(my_node_id_->index());
  if (run_sequencers_ &&
      server_settings_->sequencer == SequencerOptions::NONE) {
    ld_error("This node is configured as a sequencer, but -S option is "
             "not set");
    throw ConstructorFailed();
  }
  if (!server_settings_->audit_log.empty()) {
    audit_log_ = std::make_shared<LocalLogFile>();
    if (audit_log_->open(server_settings_->audit_log) < 0) {
      ld_error("Could not open audit log \"%s\": %s",
               server_settings_->audit_log.c_str(),
               strerror(errno));
      throw ConstructorFailed();
    };
  }

  // This is a hack to update num_logs_configured across all stat objects
  // so that aggregate returns the correct value when number of log decreases
  auto num_logs = config->localLogsConfig()->size();
  auto func = [&](logdevice::Stats& stats) {
    stats.num_logs_configured = num_logs;
  };
  getStats()->runForEach(func);

  logs_config_subscriptions_.emplace_back(
      updateable_config_->updateableLogsConfig()->subscribeToUpdates([this]() {
        std::shared_ptr<configuration::LocalLogsConfig> config =
            std::dynamic_pointer_cast<configuration::LocalLogsConfig>(
                updateable_config_->getLogsConfig());
        auto num_logs = config->size();
        auto func = [&](logdevice::Stats& stats) {
          stats.num_logs_configured = num_logs;
        };
        getStats()->runForEach(func);
      }));
}

ServerParameters::~ServerParameters() {
  server_config_subscriptions_.clear();
  logs_config_subscriptions_.clear();
  server_config_hook_handles_.clear();
  dbg::bumpErrorCounterFn = nullptr;
}

bool ServerParameters::isReadableStorageNode() const {
  return storage_node_;
}

size_t ServerParameters::getNumDBShards() const {
  return num_db_shards_;
}

bool ServerParameters::initNodesConfiguration() {
  std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory =
      getPluginRegistry()->getSinglePlugin<ZookeeperClientFactory>(
          PluginType::ZOOKEEPER_CLIENT_FACTORY);
  auto store = NodesConfigurationStoreFactory::create(
      *updateable_config_->get(),
      *getProcessorSettings().get(),
      std::move(zookeeper_client_factory));
  NodesConfigurationInit config_init(std::move(store), getProcessorSettings());
  return config_init.initWithoutProcessor(
      updateable_config_->updateableNCMNodesConfiguration());
}

bool ServerParameters::isSequencingEnabled() const {
  return run_sequencers_;
}

bool ServerParameters::isFastShutdownEnabled() const {
  return fast_shutdown_enabled_.load();
}

void ServerParameters::setFastShutdownEnabled(bool enabled) {
  fast_shutdown_enabled_.store(enabled);
}

std::shared_ptr<SettingsUpdater> ServerParameters::getSettingsUpdater() {
  return settings_updater_;
}

std::shared_ptr<UpdateableConfig> ServerParameters::getUpdateableConfig() {
  return updateable_config_;
}

std::shared_ptr<TraceLogger> ServerParameters::getTraceLogger() {
  return trace_logger_;
}

const std::shared_ptr<LocalLogFile>& ServerParameters::getAuditLog() {
  return audit_log_;
}

StatsHolder* ServerParameters::getStats() {
  return &server_stats_;
}

void ServerParameters::requestStop() {
  stop_handler_();
}

Server::Server(ServerParameters* params)
    : params_(params),
      server_settings_(params_->getServerSettings()),
      updateable_config_(params_->getUpdateableConfig()),
      server_config_(updateable_config_->getServerConfig()),
      settings_updater_(params_->getSettingsUpdater()),
      conn_budget_backlog_(server_settings_->connection_backlog),
      conn_budget_backlog_unlimited_(std::numeric_limits<uint64_t>::max()) {
  ld_check(params_);
  start_time_ = std::chrono::system_clock::now();

  if (!(initListeners() && initStore() && initProcessor() &&
        repopulateRecordCaches() && initSequencers() && initFailureDetector() &&
        initSequencerPlacement() && initRebuildingCoordinator() &&
        initClusterMaintenanceStateMachine() && initLogStoreMonitor() &&
        initUnreleasedRecordDetector() && initLogsConfigManager() &&
        initSettingsSubscriber() && initAdminServer())) {
    _exit(EXIT_FAILURE);
  }
}

template <typename T, typename... Args>
static std::unique_ptr<Listener> initListener(int port,
                                              const std::string& unix_socket,
                                              bool ssl,
                                              Args&&... args) {
  if (port > 0 || !unix_socket.empty()) {
    const auto conn_iface = unix_socket.empty()
        ? Listener::InterfaceDef(port, ssl)
        : Listener::InterfaceDef(unix_socket, ssl);

    try {
      return std::make_unique<T>(conn_iface, args...);
    } catch (const ConstructorFailed&) {
      ld_error("Failed to construct a Listener on %s",
               conn_iface.describe().c_str());
      throw;
    }
  }
  return nullptr;
}

bool Server::initListeners() {
  // create listeners (and bind to ports/socket paths specified on the command
  // line) first; exit early if ports / socket paths are taken.

  try {
    auto conn_shared_state =
        std::make_shared<ConnectionListener::SharedState>();
    connection_listener_loop_ = std::make_unique<EventLoop>(
        ConnectionListener::listenerTypeNames()
            [ConnectionListener::ListenerType::DATA],
        ThreadID::Type::UTILITY);
    connection_listener_ = initListener<ConnectionListener>(
        server_settings_->port,
        server_settings_->unix_socket,
        false,
        folly::getKeepAliveToken(connection_listener_loop_.get()),
        conn_shared_state,
        ConnectionListener::ListenerType::DATA,
        conn_budget_backlog_);
    command_listener_loop_ =
        std::make_unique<EventLoop>("ld:admin", ThreadID::Type::UTILITY);
    command_listener_ = initListener<CommandListener>(
        server_settings_->command_port,
        server_settings_->command_unix_socket,
        false,
        folly::getKeepAliveToken(command_listener_loop_.get()),
        this);

    auto nodes_configuration = updateable_config_->getNodesConfiguration();
    ld_check(nodes_configuration);
    NodeID node_id = params_->getMyNodeID().value();
    const NodeServiceDiscovery* node_svc =
        nodes_configuration->getNodeServiceDiscovery(node_id.index());
    ld_check(node_svc);

    // Gets UNIX socket or port number from a SocketAddress
    auto getSocketOrPort = [](const folly::SocketAddress& addr,
                              std::string& socket_out,
                              int& port_out) {
      socket_out.clear();
      port_out = -1;
      try {
        socket_out = addr.getPath();
      } catch (std::invalid_argument& sock_exception) {
        try {
          port_out = addr.getPort();
        } catch (std::invalid_argument& port_exception) {
          return false;
        }
      }
      return true;
    };

    if (node_svc->ssl_address.hasValue()) {
      std::string ssl_unix_socket;
      int ssl_port = -1;
      if (!getSocketOrPort(node_svc->ssl_address.value().getSocketAddress(),
                           ssl_unix_socket,
                           ssl_port)) {
        ld_error("SSL port/address couldn't be parsed for this node(%s)",
                 node_id.toString().c_str());
        return false;
      } else {
        if (!validateSSLCertificatesExist(
                params_->getProcessorSettings().get())) {
          // validateSSLCertificatesExist() should output the error
          return false;
        }
        ssl_connection_listener_loop_ = std::make_unique<EventLoop>(
            ConnectionListener::listenerTypeNames()
                [ConnectionListener::ListenerType::DATA_SSL],
            ThreadID::Type::UTILITY);
        ssl_connection_listener_ = initListener<ConnectionListener>(
            ssl_port,
            ssl_unix_socket,
            true,
            folly::getKeepAliveToken(ssl_connection_listener_loop_.get()),
            conn_shared_state,
            ConnectionListener::ListenerType::DATA_SSL,
            conn_budget_backlog_);
      }
    }

    auto gossip_sock_addr = node_svc->getGossipAddress().getSocketAddress();
    auto hostStr = node_svc->address.toString();
    auto gossip_addr_str = node_svc->getGossipAddress().toString();
    if (gossip_addr_str != hostStr) {
      std::string gossip_unix_socket;
      int gossip_port = -1;
      bool gossip_in_config =
          getSocketOrPort(gossip_sock_addr, gossip_unix_socket, gossip_port);
      if (!gossip_in_config) {
        ld_info("No gossip address/port available for node(%s) in config"
                ", can't initialize a Gossip Listener.",
                node_id.toString().c_str());
      } else if (!params_->getGossipSettings()->enabled) {
        ld_info("Not initializing a gossip listener,"
                " since gossip-enabled is not set.");
      } else {
        ld_info("Initializing a gossip listener.");
        if (params_->getProcessorSettings().get()->ssl_on_gossip_port &&
            !validateSSLCertificatesExist(
                params_->getProcessorSettings().get())) {
          // validateSSLCertificatesExist() should output the error
          return false;
        }
        gossip_listener_loop_ = std::make_unique<EventLoop>(
            ConnectionListener::listenerTypeNames()
                [ConnectionListener::ListenerType::GOSSIP],
            ThreadID::Type::UTILITY);
        gossip_listener_ = initListener<ConnectionListener>(
            gossip_port,
            gossip_unix_socket,
            false,
            folly::getKeepAliveToken(gossip_listener_loop_.get()),
            conn_shared_state,
            ConnectionListener::ListenerType::GOSSIP,
            conn_budget_backlog_unlimited_);
      }
    } else {
      ld_info("Gossip listener initialization not required"
              ", gossip_addr_str:%s",
              gossip_addr_str.c_str());
    }

  } catch (const ConstructorFailed&) {
    // failed to initialize listeners
    return false;
  }

  return true;
}

bool Server::initStore() {
  const std::string local_log_store_path =
      params_->getLocalLogStoreSettings()->local_log_store_path;
  if (params_->isReadableStorageNode()) {
    if (local_log_store_path.empty()) {
      ld_critical("This node is identified as a storage node in config (it has "
                  "a 'weight' attribute), but --local-log-store-path is not "
                  "set ");
      return false;
    }
    auto local_settings = params_->getProcessorSettings().get();
    try {
      sharded_store_.reset(
          new ShardedRocksDBLocalLogStore(local_log_store_path,
                                          params_->getNumDBShards(),
                                          *local_settings,
                                          params_->getRocksDBSettings(),
                                          params_->getRebuildingSettings(),
                                          updateable_config_,
                                          &g_rocksdb_caches,
                                          params_->getStats()));
      if (!server_settings_->ignore_cluster_marker &&
          !ClusterMarkerChecker::check(*sharded_store_,
                                       *server_config_,
                                       params_->getMyNodeID().value())) {
        ld_critical("Could not initialize log store cluster marker mismatch!");
        return false;
      }
      auto& io_fault_injection = IOFaultInjection::instance();
      io_fault_injection.init(sharded_store_->numShards());
      // Size the storage thread pool task queue to never fill up.
      size_t task_queue_size = local_settings->num_workers *
          local_settings->max_inflight_storage_tasks;
      sharded_storage_thread_pool_.reset(
          new ShardedStorageThreadPool(sharded_store_.get(),
                                       server_settings_->storage_pool_params,
                                       server_settings_,
                                       params_->getProcessorSettings(),
                                       task_queue_size,
                                       params_->getStats(),
                                       params_->getTraceLogger()));
    } catch (const ConstructorFailed& ex) {
      ld_critical("Failed to initialize local log store");
      return false;
    }
    ld_info("Initialized sharded RocksDB instance at %s with %d shards",
            local_log_store_path.c_str(),
            sharded_store_->numShards());
  }

  return true;
}

bool Server::initProcessor() {
  try {
    processor_ =
        ServerProcessor::create(params_->getAuditLog(),
                                sharded_storage_thread_pool_.get(),
                                params_->getServerSettings(),
                                params_->getGossipSettings(),
                                params_->getAdminServerSettings(),
                                updateable_config_,
                                params_->getTraceLogger(),
                                params_->getProcessorSettings(),
                                params_->getStats(),
                                params_->getPluginRegistry(),
                                "",
                                "",
                                "ld:srv", // prefix of worker thread names
                                params_->getMyNodeID());

    if (params_->getProcessorSettings()->enable_nodes_configuration_manager) {
      // create and initialize NodesConfigurationManager (NCM) and attach it to
      // the Processor

      auto my_node_id = params_->getMyNodeID().value();
      auto node_svc_discovery =
          updateable_config_->getNodesConfiguration()->getNodeServiceDiscovery(
              my_node_id);
      if (node_svc_discovery == nullptr) {
        ld_critical(
            "NodeID '%s' doesn't exist in the NodesConfiguration of %s",
            my_node_id.toString().c_str(),
            updateable_config_->getServerConfig()->getClusterName().c_str());
        throw ConstructorFailed();
      }
      auto roleset = node_svc_discovery->getRoles();

      // TODO: get NCS from NodesConfigurationInit instead
      auto zk_client_factory = processor_->getPluginRegistry()
                                   ->getSinglePlugin<ZookeeperClientFactory>(
                                       PluginType::ZOOKEEPER_CLIENT_FACTORY);
      auto ncm = configuration::nodes::NodesConfigurationManagerFactory::create(
          processor_.get(), nullptr, roleset, std::move(zk_client_factory));
      if (ncm == nullptr) {
        ld_critical("Unable to create NodesConfigurationManager during server "
                    "creation!");
        throw ConstructorFailed();
      }

      auto initial_nc =
          processor_->config_->getNodesConfigurationFromNCMSource();
      if (!initial_nc) {
        // Currently this should only happen in tests as our boostrapping
        // workflow should always ensure the Processor has a valid
        // NodesConfiguration before initializing NCM. In the future we will
        // require a valid NC for Processor construction and will turn this into
        // a ld_check.
        ld_warning("NodesConfigurationManager initialized without a valid "
                   "NodesConfiguration in its Processor context. This should "
                   "only happen in tests.");
        initial_nc = std::make_shared<const NodesConfiguration>();
      }
      if (!ncm->init(std::move(initial_nc))) {
        ld_critical(
            "Processing initial NodesConfiguration did not finish in time.");
        throw ConstructorFailed();
      }
    }

    if (sharded_storage_thread_pool_) {
      sharded_storage_thread_pool_->setProcessor(processor_.get());

      // Give sharded_store_ a pointer to the thread pool, after
      // the thread pool has a pointer to Processor.
      sharded_store_->setShardedStorageThreadPool(
          sharded_storage_thread_pool_.get());
    }

    processor_->setServerInstanceId(
        SystemTimestamp::now().toMilliseconds().count());
  } catch (const ConstructorFailed&) {
    ld_error("Failed to construct a Processor: error %d (%s)",
             static_cast<int>(err),
             error_description(err));
    return false;
  }
  return true;
}

bool Server::repopulateRecordCaches() {
  if (!params_->isReadableStorageNode()) {
    ld_info("Not repopulating record caches");
    return true;
  }

  // Callback function for each status
  std::map<Status, std::vector<int>> status_counts;
  auto callback = [&status_counts](Status status, shard_index_t shard_idx) {
    status_counts[status].push_back(shard_idx);
  };

  // Start RecordCacheRepopulationRequest. Only try to deserialize record cache
  // snapshot if record cache is enabled _and_ persisting record cache is
  // allowed. Otherwise, just drop all previous snapshots.
  std::unique_ptr<Request> req =
      std::make_unique<RepopulateRecordCachesRequest>(
          callback, params_->getProcessorSettings()->enable_record_cache);
  if (processor_->blockingRequest(req) != 0) {
    ld_critical("Failed to make a blocking request to repopulate record "
                "caches!");
    return false;
  }

  int num_failed_deletions = status_counts[E::FAILED].size();
  int num_failed_repopulations = status_counts[E::PARTIAL].size();
  int num_disabled_shards = status_counts[E::DISABLED].size();
  // Sanity check that no other status is used
  ld_check(num_failed_deletions + num_failed_repopulations +
               num_disabled_shards + status_counts[E::OK].size() ==
           params_->getNumDBShards());

  auto getAffectedShards = [&](Status status) {
    std::string result;
    folly::join(", ", status_counts[status], result);
    return result;
  };

  if (num_failed_deletions) {
    ld_critical("Failed to delete snapshots on the following enabled shards: "
                "[%s]",
                getAffectedShards(E::FAILED).c_str());
    return false;
  }
  if (num_failed_repopulations) {
    ld_error("Failed to repopulate all snapshot(s) due to data corruption or "
             "size limit, leaving caches empty or paritally populated. "
             "Affected shards: [%s]",
             getAffectedShards(E::PARTIAL).c_str());
  }
  if (num_disabled_shards) {
    ld_info("Did not repopulate record caches from disabled shards: [%s]",
            getAffectedShards(E::DISABLED).c_str());
  }
  return true;
}

bool Server::initSequencers() {
  // Create an instance of EpochStore.
  std::unique_ptr<EpochStore> epoch_store;

  if (!server_settings_->epoch_store_path.empty()) {
    try {
      ld_info("Initializing FileEpochStore");
      epoch_store = std::make_unique<FileEpochStore>(
          server_settings_->epoch_store_path,
          processor_.get(),
          updateable_config_->updateableNodesConfiguration());
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct FileEpochStore: %s", error_description(err));
      return false;
    }
  } else {
    ld_info("Initializing ZookeeperEpochStore");
    try {
      std::shared_ptr<ZookeeperClientFactory> zk_client_factory =
          processor_->getPluginRegistry()
              ->getSinglePlugin<ZookeeperClientFactory>(
                  PluginType::ZOOKEEPER_CLIENT_FACTORY);
      epoch_store = std::make_unique<ZookeeperEpochStore>(
          server_config_->getClusterName(),
          processor_.get(),
          updateable_config_->updateableZookeeperConfig(),
          updateable_config_->updateableNodesConfiguration(),
          processor_->updateableSettings(),
          zk_client_factory);
    } catch (const ConstructorFailed&) {
      ld_error("Failed to construct ZookeeperEpochStore: %s",
               error_description(err));
      return false;
    }
  }

  processor_->allSequencers().setEpochStore(std::move(epoch_store));

  return true;
}

bool Server::initLogStoreMonitor() {
  if (params_->isReadableStorageNode()) {
    logstore_monitor_ = std::make_unique<LogStoreMonitor>(
        processor_.get(), params_->getLocalLogStoreSettings());
    logstore_monitor_->start();
  }

  return true;
}

bool Server::initSequencerPlacement() {
  // SequencerPlacement has a pointer to Processor and will notify it of
  // placement updates
  if (params_->isSequencingEnabled()) {
    try {
      std::shared_ptr<SequencerPlacement> placement_ptr;

      switch (server_settings_->sequencer) {
        case SequencerOptions::ALL:
          ld_info("using SequencerOptions::ALL");
          placement_ptr =
              std::make_shared<StaticSequencerPlacement>(processor_.get());
          break;

        case SequencerOptions::LAZY:
          ld_info("using SequencerOptions::LAZY");
          placement_ptr = std::make_shared<LazySequencerPlacement>(
              processor_.get(), params_->getGossipSettings());
          break;

        case SequencerOptions::NONE:
          ld_check(false);
          break;
      }

      sequencer_placement_.update(std::move(placement_ptr));
    } catch (const ConstructorFailed& ex) {
      ld_error("Failed to initialize SequencerPlacement object");
      return false;
    }
  }

  return true;
}

bool Server::initRebuildingCoordinator() {
  std::shared_ptr<Configuration> config = processor_->config_->get();

  bool enable_rebuilding = false;
  if (params_->getRebuildingSettings()->disable_rebuilding) {
    ld_info("Rebuilding is disabled.");
  } else if (!config->logsConfig()->logExists(
                 configuration::InternalLogs::EVENT_LOG_DELTAS)) {
    ld_error("No event log is configured but rebuilding is enabled. Configure "
             "an event log by populating the \"internal_logs\" section of the "
             "server config and restart this server");
  } else {
    enable_rebuilding = true;
    event_log_ =
        std::make_unique<EventLogStateMachine>(params_->getProcessorSettings());
    event_log_->enableSendingUpdatesToWorkers();
    event_log_->setMyNodeID(params_->getMyNodeID().value());
  }

  if (sharded_store_) {
    if (!enable_rebuilding) {
      // We are not enabling rebuilding. Notify Processor that all
      // shards are authoritative.
      for (uint32_t shard = 0; shard < sharded_store_->numShards(); ++shard) {
        getProcessor()->markShardAsNotMissingData(shard);
        getProcessor()->markShardClean(shard);
      }
    } else {
      ld_check(event_log_);

      rebuilding_supervisor_ = std::make_unique<RebuildingSupervisor>(
          event_log_.get(),
          processor_.get(),
          params_->getRebuildingSettings(),
          params_->getAdminServerSettings());
      processor_->rebuilding_supervisor_ = rebuilding_supervisor_.get();
      ld_info("Starting RebuildingSupervisor");
      rebuilding_supervisor_->start();

      rebuilding_coordinator_ = std::make_unique<RebuildingCoordinator>(
          processor_->config_,
          event_log_.get(),
          processor_.get(),
          params_->getRebuildingSettings(),
          params_->getAdminServerSettings(),
          sharded_store_.get());
      ld_info("Starting RebuildingCoordinator");
      if (rebuilding_coordinator_->start() != 0) {
        return false;
      }
    }
  }

  if (event_log_) {
    std::unique_ptr<Request> req =
        std::make_unique<StartEventLogStateMachineRequest>(event_log_.get(), 0);

    const int rv = processor_->postRequest(req);
    if (rv != 0) {
      ld_error("Cannot post request to start event log state machine: %s (%s)",
               error_name(err),
               error_description(err));
      ld_check(false);
      return false;
    }
  }

  return true;
}

bool Server::createAndAttachMaintenanceManager(AdminServer* admin_server) {
  // MaintenanceManager can generally be run on any server. However
  // MaintenanceManager lacks the leader election logic and hence
  // we cannot have multiple MaintenanceManager-s running for the
  // same cluster. To avoid this, we do want MaintenanceManager run
  // on regular logdevice servers except for purpose of testing, where
  // the node that should run a instance of MaintenanceManager can be
  // directly controlled.
  const auto admin_settings = params_->getAdminServerSettings();
  if (admin_settings->enable_maintenance_manager) {
    ld_check(cluster_maintenance_state_machine_);
    ld_check(event_log_);
    ld_check(admin_server);
    auto deps = std::make_unique<maintenance::MaintenanceManagerDependencies>(
        processor_.get(),
        admin_settings,
        cluster_maintenance_state_machine_.get(),
        event_log_.get(),
        std::make_unique<maintenance::SafetyCheckScheduler>(
            processor_.get(),
            admin_settings,
            admin_server->getSafetyChecker()));
    auto worker_idx = processor_->selectWorkerRandomly(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS.val_ /*seed*/,
        maintenance::MaintenanceManager::workerType(processor_.get()));
    auto& w = processor_->getWorker(
        worker_idx,
        maintenance::MaintenanceManager::workerType(processor_.get()));
    maintenance_manager_ =
        std::make_unique<maintenance::MaintenanceManager>(&w, std::move(deps));
    admin_server->setMaintenanceManager(maintenance_manager_.get());
    // Since this node is going to run MaintenanceManager, upgrade the
    // NodesConfigManager to be a proposer
    auto ncm = processor_->getNodesConfigurationManager();
    ld_check(ncm);
    ncm->upgradeToProposer();
    maintenance_manager_->start();
  } else {
    ld_info(
        "Not initializing MaintenanceManager since it is disabled in settings");
  }
  return true;
}

bool Server::initClusterMaintenanceStateMachine() {
  if (params_->getAdminServerSettings()
          ->enable_cluster_maintenance_state_machine ||
      params_->getAdminServerSettings()->enable_maintenance_manager) {
    cluster_maintenance_state_machine_ =
        std::make_unique<maintenance::ClusterMaintenanceStateMachine>(
            params_->getAdminServerSettings());

    std::unique_ptr<Request> req = std::make_unique<
        maintenance::StartClusterMaintenanceStateMachineRequest>(
        cluster_maintenance_state_machine_.get(),
        maintenance::ClusterMaintenanceStateMachine::workerType(
            processor_.get()));

    const int rv = processor_->postRequest(req);
    if (rv != 0) {
      ld_error("Cannot post request to start cluster maintenance state "
               "machine: %s (%s)",
               error_name(err),
               error_description(err));
      ld_check(false);
      return false;
    }
  }
  return true;
}

bool Server::initFailureDetector() {
  if (params_->getGossipSettings()->enabled) {
    try {
      processor_->failure_detector_ = std::make_unique<FailureDetector>(
          params_->getGossipSettings(), processor_.get(), params_->getStats());
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct FailureDetector: %s", error_description(err));
      return false;
    }
  } else {
    ld_info("Not initializing gossip based failure detector,"
            " since --gossip-enabled is not set");
  }

  return true;
}

bool Server::initUnreleasedRecordDetector() {
  if (params_->isReadableStorageNode()) {
    unreleased_record_detector_ = std::make_shared<UnreleasedRecordDetector>(
        processor_.get(), params_->getProcessorSettings());
    unreleased_record_detector_->start();
  }

  return true;
}

bool Server::startCommandListener(std::unique_ptr<Listener>& handle) {
  CommandListener* listener = checked_downcast<CommandListener*>(handle.get());
  return listener->startAcceptingConnections().wait().value();
}

bool Server::startConnectionListener(std::unique_ptr<Listener>& handle) {
  ConnectionListener* listener =
      checked_downcast<ConnectionListener*>(handle.get());
  listener->setProcessor(processor_.get());
  return listener->startAcceptingConnections().wait().value();
}

bool Server::initLogsConfigManager() {
  return LogsConfigManager::createAndAttach(
      *processor_, true /* is_writable */);
}

bool Server::initSettingsSubscriber() {
  auto settings = params_->getProcessorSettings();
  settings_subscription_handle_ =
      settings.subscribeToUpdates([&] { updateStatsSettings(); });

  return true;
}

bool Server::initAdminServer() {
  if (params_->getServerSettings()->admin_enabled) {
    // Figure out the socket address for the admin server.
    auto server_config = updateable_config_->getServerConfig();
    ld_check(server_config);

    auto adm_plugin =
        params_->getPluginRegistry()->getSinglePlugin<AdminServerFactory>(
            PluginType::ADMIN_SERVER_FACTORY);
    if (adm_plugin) {
      admin_server_handle_ = (*adm_plugin)(processor_.get(),
                                           params_->getSettingsUpdater(),
                                           params_->getServerSettings(),
                                           params_->getAdminServerSettings(),
                                           params_->getStats());
    } else {
      // Use built-in SimpleAdminServer
      admin_server_handle_ =
          std::make_unique<SimpleAdminServer>(processor_.get(),
                                              params_->getSettingsUpdater(),
                                              params_->getServerSettings(),
                                              params_->getAdminServerSettings(),
                                              params_->getStats());
    }
    if (sharded_store_) {
      admin_server_handle_->setShardedRocksDBStore(sharded_store_.get());
    }
    createAndAttachMaintenanceManager(admin_server_handle_.get());
  } else {
    ld_info("Not initializing Admin API,"
            " since admin-enabled server setting is set to false");
  }
  return true;
}

bool Server::startListening() {
  // start accepting new connections
  if (!startConnectionListener(connection_listener_)) {
    return false;
  }

  if (gossip_listener_loop_ && !startConnectionListener(gossip_listener_)) {
    return false;
  }

  if (ssl_connection_listener_loop_ &&
      !startConnectionListener(ssl_connection_listener_)) {
    return false;
  }

  if (admin_server_handle_ && !admin_server_handle_->start()) {
    return false;
  }

  // start command listener last, so that integration test framework
  // cannot connect to the command port in the event that any other port
  // failed to open.
  if (!startCommandListener(command_listener_)) {
    return false;
  }

  return true;
}

void Server::requestStop() {
  params_->requestStop();
}

void Server::gracefulShutdown() {
  if (is_shut_down_.exchange(true)) {
    return;
  }
  shutdown_server(admin_server_handle_,
                  connection_listener_,
                  command_listener_,
                  gossip_listener_,
                  ssl_connection_listener_,
                  connection_listener_loop_,
                  command_listener_loop_,
                  gossip_listener_loop_,
                  ssl_connection_listener_loop_,
                  logstore_monitor_,
                  processor_,
                  sharded_storage_thread_pool_,
                  sharded_store_,
                  sequencer_placement_.get(),
                  rebuilding_coordinator_,
                  event_log_,
                  rebuilding_supervisor_,
                  unreleased_record_detector_,
                  cluster_maintenance_state_machine_,
                  params_->isFastShutdownEnabled());
}

void Server::shutdownWithTimeout() {
  bool done = false;
  std::condition_variable cv;
  std::mutex mutex;

  // perform all work in a separate thread so that we can specify a timeout
  std::thread thread([&]() {
    ThreadID::set(ThreadID::Type::UTILITY, "ld:shtdwn-timer");
    std::unique_lock<std::mutex> lock(mutex);
    if (!done && !cv.wait_for(lock, server_settings_->shutdown_timeout, [&]() {
          return done;
        })) {
      ld_warning("Timeout expired while waiting for shutdown to complete");
      fflush(stdout);
      // Make sure to dump a core to make it easier to investigate.
      std::abort();
    }
  });

  {
    gracefulShutdown();
    {
      std::lock_guard<std::mutex> lock(mutex);
      done = true;
    }
    cv.notify_one();
  }

  thread.join();
}

Processor* Server::getProcessor() const {
  return processor_.get();
}

RebuildingCoordinator* Server::getRebuildingCoordinator() {
  return rebuilding_coordinator_.get();
}

maintenance::MaintenanceManager* Server::getMaintenanceManager() {
  return maintenance_manager_.get();
}

void Server::rotateLocalLogs() {
  auto audit_log = params_->getAuditLog();
  if (audit_log) {
    audit_log->reopen();
  }
}

void Server::updateStatsSettings() {
  const auto retention_time =
      params_->getProcessorSettings()
          ->sequencer_boycotting.node_stats_retention_on_nodes;
  auto stats = params_->getStats();
  auto shared_params = stats->params_.get();

  if (shared_params->node_stats_retention_time_on_nodes != retention_time) {
    auto params = *shared_params;
    params.node_stats_retention_time_on_nodes = retention_time;

    stats->params_.update(std::make_shared<StatsParams>(std::move(params)));
    stats->runForEach([&](auto& stats) {
      stats.per_client_node_stats.wlock()->updateRetentionTime(retention_time);
    });
  }
}

Server::~Server() {
  shutdownWithTimeout();
}

}} // namespace facebook::logdevice
