/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "logdevice/admin/AdminServer.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/StatsCollectionThread.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {
class ClientImpl;
class SettingsUpdater;

namespace maintenance {
class MaintenanceManager;
class ClusterMaintenanceStateMachine;
} // namespace maintenance

namespace configuration { namespace nodes {
class NodesConfigurationStore;
}} // namespace configuration::nodes

namespace admin {

// Exception that can be thrown in start();
struct StandaloneAdminServerFailed : std::exception {};

class StandaloneAdminServer {
 public:
  StandaloneAdminServer(std::shared_ptr<PluginRegistry> plugins,
                        std::shared_ptr<SettingsUpdater> settings_updater);
  void start();
  std::shared_ptr<SettingsUpdater> getSettingsUpdater() const {
    return settings_updater_;
  }
  void shutdown();
  void waitForShutdown();

 private:
  std::shared_ptr<PluginRegistry> plugin_registry_;
  std::shared_ptr<SettingsUpdater> settings_updater_;

  UpdateableSettings<ServerSettings> server_settings_;
  UpdateableSettings<AdminServerSettings> admin_settings_;
  UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  UpdateableSettings<LocalLogStoreSettings> locallogstore_settings_;
  UpdateableSettings<GossipSettings> gossip_settings_;
  UpdateableSettings<Settings> settings_;
  UpdateableSettings<RocksDBSettings> rocksdb_settings_;

  UpdateableSettings<ServerSettings>::SubscriptionHandle
      server_settings_subscription_;

  std::shared_ptr<UpdateableConfig> updateable_config_;

  UpdateableServerConfig::HookHandle server_config_subscription_;
  UpdateableNodesConfiguration::HookHandle nodes_configuration_subscription_;

  std::unique_ptr<StatsHolder> stats_;
  std::unique_ptr<StatsCollectionThread> stats_thread_;
  std::unique_ptr<AdminServer> admin_server_;
  std::unique_ptr<maintenance::ClusterMaintenanceStateMachine>
      cluster_maintenance_state_machine_;
  std::unique_ptr<maintenance::MaintenanceManager> maintenance_manager_;
  std::shared_ptr<ClientProcessor> processor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> cpu_executor_;
  std::unique_ptr<EventLogStateMachine> event_log_;
  // After initializing all threads
  Semaphore main_thread_sem_;
  std::atomic<bool> shutdown_requested_{false};

  void initServerConfig();
  void initNodesConfiguration();
  void initStatsCollection();
  void initProcessor();
  void initNodesConfigurationManager();
  void initLogsConfigManager();
  void initClusterStateRefresher();
  void initAdminServer();
  void initEventLog();
  void initMaintenanceManager();
  void initClusterMaintenanceStateMachine();
  // Creates maintenance manager if it is enabled in settings
  // starts it on a random worker and sets a handle on AdminServer
  void createAndAttachMaintenanceManager(AdminServer* server);
  // Gets called whenever ServerSettings is updated, this is also called on
  // startup to set the initial values supplied from the CLI.
  void onSettingsUpdate();

  // Is used to update settings when configuration is updated
  bool onConfigUpdate(ServerConfig& config);
  bool onNodesConfigurationUpdate(const NodesConfiguration& config);

  // Validates that all the nodes in the config have names. This is specially
  // important to the standalone admin server because tooling relies on the
  // existence of names and config synchronization was poisoning the config.
  // TODO(T44427489): Remove when the `name` is required everywhere.
  bool allNodesHaveName(const NodesConfiguration& config);

  // Builds an an admin client based NodesConfigurationStore
  std::unique_ptr<configuration::nodes::NodesConfigurationStore>
  buildNodesConfigurationStore();
};
} // namespace admin
}} // namespace facebook::logdevice
