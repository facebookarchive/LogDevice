/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Server.h"

#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/StaticSequencerPlacement.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/ZookeeperEpochStore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/settings/SSLSettingValidation.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/LazySequencerPlacement.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/MyNodeID.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerPluginPack.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/UnreleasedRecordDetector.h"
#include "logdevice/server/fatalsignal.h"
#include "logdevice/server/locallogstore/ClusterMarkerChecker.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/RecordCacheRepopulationTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/util.h"

using facebook::logdevice::configuration::LocalLogsConfig;

namespace facebook { namespace logdevice {

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

// verifies that the number of nodes in the cluster is at most max_nodes
bool ServerParameters::validateNodes(ServerConfig& config) {
  if (config.getMaxNodeIdx() + 1 > processor_settings_->max_nodes) {
    ld_error("Invalid config: max node index (%zu) exceeds "
             "max_nodes (%zu)",
             config.getMaxNodeIdx(),
             processor_settings_->max_nodes);

    return false;
  }
  return true;
}

// recalculates server's node id after every config update
bool ServerParameters::updateMyNodeId(ServerConfig& config) {
  NodeID node_id;
  if (my_node_id_extractor_->calculate(config, node_id) != 0) {
    return false;
  }
  if (my_node_index_.hasValue()) {
    if (my_node_index_.value() != node_id.index()) {
      ld_error("My node index changed from %d to %d. Rejecting config.",
               (int)my_node_index_.value(),
               (int)node_id.index());

      return false;
    }
  } else {
    my_node_index_ = node_id.index();
  }
  config.setMyNodeID(node_id);
  if (!Address(config.getServerOrigin()).valid()) {
    // Update the server origin of the config to my node ID if it is not
    // set already
    config.setServerOrigin(node_id);
  }
  return true;
}

bool ServerParameters::updateConfigSettings(ServerConfig& config) {
  // Merge the main settings section and the settings section for my node.
  // Note that calling getMyNodeID() is safe because we expect updateMyNodeId()
  // was already called.
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Updating settings from config took %lums",
            msec_since(start_ts.timePoint()));
  };
  auto settings = config.getServerSettingsConfig();
  const configuration::Node* me = config.getNode(config.getMyNodeID().index());
  ld_check(me);
  for (auto s : me->settings) {
    settings[s.first] = s.second;
  }

  try {
    settings_updater_->setFromConfig(settings);
  } catch (const boost::program_options::error&) {
    return false;
  }
  return true;
}

bool ServerParameters::setConnectionLimits() {
  if (server_settings_->fd_limit == 0 ||
      server_settings_->num_reserved_fds == 0) {
    ld_debug("not enforcing limits on incoming connections");
    return true;
  }

  auto config = updateable_config_->get();
  std::shared_ptr<const Settings> settings = processor_settings_.get();

  const size_t nodes = config->serverConfig()->getNodes().size();
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

ServerParameters::ServerParameters(
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    UpdateableSettings<LocalLogStoreSettings> locallogstore_settings,
    UpdateableSettings<GossipSettings> gossip_settings,
    UpdateableSettings<Settings> processor_settings,
    UpdateableSettings<RocksDBSettings> rocksdb_settings,
    std::shared_ptr<ServerPluginPack> plugin)
    : plugin_(std::move(plugin)),
      server_stats_(StatsParams().setIsServer(true)),
      settings_updater_(std::move(settings_updater)),
      server_settings_(std::move(server_settings)),
      rebuilding_settings_(std::move(rebuilding_settings)),
      locallogstore_settings_(std::move(locallogstore_settings)),
      gossip_settings_(std::move(gossip_settings)),
      processor_settings_(std::move(processor_settings)),
      rocksdb_settings_(std::move(rocksdb_settings)) {
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

  if (!server_settings_->unix_socket.empty()) {
    my_node_id_extractor_ =
        std::make_unique<MyNodeID>(server_settings_->unix_socket);
  } else {
    my_node_id_extractor_ = std::make_unique<MyNodeID>(server_settings_->port);
  }

  auto updateable_server_config = std::make_shared<UpdateableServerConfig>();
  auto updateable_logs_config = std::make_shared<UpdateableLogsConfig>();
  auto updatable_zookeeper_config =
      std::make_shared<UpdateableZookeeperConfig>();
  updateable_config_ =
      std::make_shared<UpdateableConfig>(updateable_server_config,
                                         updateable_logs_config,
                                         updatable_zookeeper_config);
  server_config_hook_handles_.push_back(
      updateable_server_config->addHook(std::bind(
          &ServerParameters::updateMyNodeId, this, std::placeholders::_1)));
  server_config_hook_handles_.push_back(updateable_server_config->addHook(
      std::bind(&ServerParameters::updateConfigSettings,
                this,
                std::placeholders::_1)));
  server_config_hook_handles_.push_back(
      updateable_server_config->addHook(std::bind(
          &ServerParameters::validateNodes, this, std::placeholders::_1)));

  {
    ConfigInit config_init(
        processor_settings_->initial_config_load_timeout, getStats());
    config_init.setFilePollingInterval(
        processor_settings_->file_config_update_interval);
    config_init.setZookeeperPollingInterval(
        processor_settings_->zk_config_polling_interval);
    int rv = config_init.attach(server_settings_->config_path,
                                plugin_,
                                updateable_config_,
                                nullptr,
                                processor_settings_);
    if (rv != 0) {
      throw ConstructorFailed();
    }
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

  NodeID node_id = config->serverConfig()->getMyNodeID();
  ld_info("My NodeID is %s", node_id.toString().c_str());
  const ServerConfig::Node* this_node =
      config->serverConfig()->getNode(node_id);
  ld_check(this_node != nullptr);

  if (!setConnectionLimits()) {
    throw ConstructorFailed();
  }

  // Construct the Server Trace Logger
  if (processor_settings_->trace_logger_disabled) {
    trace_logger_ = std::make_shared<NoopTraceLogger>(updateable_config_);
  } else {
    trace_logger_ = plugin_->createTraceLogger(updateable_config_);
  }

  storage_node_ = this_node->hasRole(Configuration::NodeRole::STORAGE);
  num_db_shards_ = this_node->getNumShards();

  run_sequencers_ = this_node->isSequencingEnabled();
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

Server::Server(ServerParameters* params, std::function<void()> stop_handler)
    : params_(params),
      stop_handler_(stop_handler),
      server_settings_(params_->getServerSettings()),
      updateable_config_(params_->getUpdateableConfig()),
      server_config_(updateable_config_->getServerConfig()),
      settings_updater_(params_->getSettingsUpdater()) {
  ld_check(params_);
  ld_check(stop_handler_);
  start_time_ = std::chrono::system_clock::now();

  if (!(initListeners() && initStore() && initProcessor() &&
        repopulateRecordCaches() && initSequencers() && initFailureDetector() &&
        initSequencerPlacement() && initRebuildingCoordinator() &&
        initLogStoreMonitor() && initUnreleasedRecordDetector() &&
        initLogsConfigManager() && initSettingsSubscriber())) {
    _exit(EXIT_FAILURE);
  }
}

template <typename T, typename... Args>
static std::unique_ptr<EventLoopHandle>
initListener(int port,
             const std::string& unix_socket,
             bool ssl,
             Args&&... args) {
  if (port > 0 || !unix_socket.empty()) {
    const auto conn_iface = unix_socket.empty()
        ? Listener::InterfaceDef(port, ssl)
        : Listener::InterfaceDef(unix_socket, ssl);

    try {
      return std::make_unique<EventLoopHandle>(new T(conn_iface, args...));
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
    connection_listener_handle_ = initListener<ConnectionListener>(
        server_settings_->port,
        server_settings_->unix_socket,
        false,
        conn_shared_state,
        ConnectionListener::ListenerType::DATA);
    command_listener_handle_ =
        initListener<CommandListener>(server_settings_->command_port,
                                      server_settings_->command_unix_socket,
                                      false,
                                      this);

    std::shared_ptr<Configuration> config = updateable_config_->get();
    ld_check(config);
    NodeID node_id = config->serverConfig()->getMyNodeID();
    const ServerConfig::Node* node_config =
        config->serverConfig()->getNode(node_id);

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

    if (node_config->ssl_address.hasValue()) {
      std::string ssl_unix_socket;
      int ssl_port = -1;
      if (!getSocketOrPort(node_config->ssl_address.value().getSocketAddress(),
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
        ssl_connection_listener_handle_ = initListener<ConnectionListener>(
            ssl_port,
            ssl_unix_socket,
            true,
            conn_shared_state,
            ConnectionListener::ListenerType::DATA_SSL);
      }
    }

    auto gossip_sock_addr = node_config->gossip_address.getSocketAddress();
    auto hostStr = node_config->address.toString();
    auto gossip_addr_str = node_config->gossip_address.toString();
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
        gossip_listener_handle_ = initListener<ConnectionListener>(
            gossip_port,
            gossip_unix_socket,
            false,
            conn_shared_state,
            ConnectionListener::ListenerType::GOSSIP);
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
      ld_error("This node is identified as a storage node in config (it has a "
               "'weight' attribute), but --local-log-store-path is not set ");
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
          !ClusterMarkerChecker::check(*sharded_store_, *server_config_)) {
        return false;
      }
      io_fault_injection.init(sharded_store_->numShards());
      // Size the storage thread pool task queue to never fill up.
      size_t task_queue_size = local_settings->num_workers *
          local_settings->max_inflight_storage_tasks;
      sharded_storage_thread_pool_.reset(new ShardedStorageThreadPool(
          sharded_store_.get(),
          server_settings_->shard_storage_pool_params,
          params_->getProcessorSettings(),
          task_queue_size,
          params_->getStats(),
          params_->getTraceLogger()));
    } catch (const ConstructorFailed& ex) {
      ld_error("Failed to initialize local log store");
      return false;
    }
    ld_info("Initialized sharded RocksDB instance at %s with %d shards",
            local_log_store_path.c_str(),
            sharded_store_->numShards());
  }

  return true;
}

bool Server::initProcessor() {
  std::shared_ptr<ServerPluginPack> plugin = params_->getPlugin();
  ld_check(plugin);
  std::unique_ptr<SequencerLocator> sequencer_locator =
      plugin->createSequencerLocator(updateable_config_);

  try {
    processor_ =
        ServerProcessor::create(params_->getAuditLog(),
                                sharded_storage_thread_pool_.get(),
                                params_->getServerSettings(),
                                params_->getGossipSettings(),
                                updateable_config_,
                                params_->getTraceLogger(),
                                params_->getProcessorSettings(),
                                params_->getStats(),
                                std::move(sequencer_locator),
                                params_->getPlugin(),
                                "",
                                "",
                                "ld:srv" // prefix of worker thread names
        );

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

  if (params_->getServerSettings()->admin_enabled) {
    admin_server_handle_ = plugin->createAdminServer(this);
  } else {
    ld_info("Not initializing Admin API,"
            " since admin-enabled server setting is set to false");
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
          updateable_config_->updateableServerConfig());
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct FileEpochStore: %s", error_description(err));
      return false;
    }
  } else {
    ld_info("Initializing ZookeeperEpochStore");
    try {
      epoch_store = std::make_unique<ZookeeperEpochStore>(
          server_config_->getClusterName(),
          processor_.get(),
          updateable_config_->updateableZookeeperConfig(),
          updateable_config_->updateableServerConfig(),
          processor_->updateableSettings(),
          zkFactoryProd);
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

  std::unique_ptr<EventLogStateMachine> event_log;

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
    event_log =
        std::make_unique<EventLogStateMachine>(params_->getProcessorSettings());
    event_log->enableSendingUpdatesToWorkers();
    event_log->setMyNodeID(config->serverConfig()->getMyNodeID());
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
      ld_check(event_log);

      rebuilding_supervisor_ = std::make_unique<RebuildingSupervisor>(
          event_log.get(), processor_.get(), params_->getRebuildingSettings());
      processor_->rebuilding_supervisor_ = rebuilding_supervisor_.get();
      ld_info("Starting RebuildingSupervisor");
      rebuilding_supervisor_->start();

      rebuilding_coordinator_ = std::make_unique<RebuildingCoordinator>(
          processor_->config_,
          event_log.get(),
          processor_.get(),
          params_->getRebuildingSettings(),
          sharded_store_.get());
      ld_info("Starting RebuildingCoordinator");
      if (rebuilding_coordinator_->start() != 0) {
        return false;
      }
    }
  }

  if (event_log) {
    std::unique_ptr<Request> req =
        std::make_unique<StartEventLogStateMachineRequest>(
            std::move(event_log), 0);

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

bool Server::startCommandListener(std::unique_ptr<EventLoopHandle>& handle) {
  CommandListener* listener = checked_downcast<CommandListener*>(handle->get());
  if (listener->startAcceptingConnections() < 0) {
    return false;
  }
  handle->start();
  return true;
}

bool Server::startConnectionListener(std::unique_ptr<EventLoopHandle>& handle) {
  ConnectionListener* listener =
      checked_downcast<ConnectionListener*>(handle->get());
  listener->setProcessor(processor_.get());
  if (listener->startAcceptingConnections() < 0) {
    return false;
  }
  handle->start();
  return true;
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

bool Server::startListening() {
  // start accepting new connections
  if (!startConnectionListener(connection_listener_handle_)) {
    return false;
  }

  if (gossip_listener_handle_ &&
      !startConnectionListener(gossip_listener_handle_)) {
    return false;
  }

  if (ssl_connection_listener_handle_ &&
      !startConnectionListener(ssl_connection_listener_handle_)) {
    return false;
  }

  // start command listener last, so that integration test framework
  // cannot connect to the command port in the event that any other port
  // failed to open.
  if (!startCommandListener(command_listener_handle_)) {
    return false;
  }

  if (admin_server_handle_ && !admin_server_handle_->start()) {
    return false;
  }

  return true;
}

void Server::requestStop() {
  stop_handler_();
}

void Server::gracefulShutdown() {
  if (is_shut_down_.exchange(true)) {
    return;
  }
  shutdown_server(admin_server_handle_,
                  connection_listener_handle_,
                  command_listener_handle_,
                  gossip_listener_handle_,
                  ssl_connection_listener_handle_,
                  logstore_monitor_,
                  processor_,
                  sharded_storage_thread_pool_,
                  sharded_store_,
                  sequencer_placement_.get(),
                  rebuilding_coordinator_,
                  rebuilding_supervisor_,
                  unreleased_record_detector_,
                  params_->isFastShutdownEnabled());
}

void Server::shutdownWithTimeout() {
  bool done = false;
  std::condition_variable cv;
  std::mutex mutex;

  // perform all work in a separate thread so that we can specify a timeout
  std::thread thread([&]() {
    ThreadID::set(ThreadID::Type::UTILITY, "ld:shutdown");
    gracefulShutdown();

    {
      std::lock_guard<std::mutex> lock(mutex);
      done = true;
    }
    cv.notify_one();
  });

  {
    std::unique_lock<std::mutex> lock(mutex);
    if (!cv.wait_for(
            lock, server_settings_->shutdown_timeout, [&]() { return done; })) {
      ld_warning("Timeout expired while waiting for shutdown to complete");
      fflush(stdout);
      // Make sure to dump a core to make it easier to investigate.
      std::abort();
    }
  }

  thread.join();
}

Processor* Server::getProcessor() const {
  return processor_.get();
}

RebuildingCoordinator* Server::getRebuildingCoordinator() {
  return rebuilding_coordinator_.get();
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
      for (auto& thread_stats :
           stats.synchronizedCopy(&Stats::per_client_node_stats)) {
        thread_stats.second->updateRetentionTime(retention_time);
      }
    });
  }
}

Server::~Server() {
  shutdownWithTimeout();
}

}} // namespace facebook::logdevice
