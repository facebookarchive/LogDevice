/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "TestUtil.h"

#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/admin/AdminServer.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerPluginPack.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

class TestServerPluginPack : public ServerPluginPack {
 public:
  virtual const char* description() const override {
    return "testing server plugin";
  }
};

std::shared_ptr<ServerPluginPack> make_test_server_plugin_pack() {
  return std::make_shared<TestServerPluginPack>();
}

std::shared_ptr<ServerProcessor> make_test_server_processor(
    const Settings& settings,
    const ServerSettings& server_settings,
    const GossipSettings& gossip_settings,
    std::shared_ptr<UpdateableConfig> config,
    ShardedStorageThreadPool* sharded_storage_thread_pool,
    StatsHolder* stats) {
  if (!config) {
    config = UpdateableConfig::createEmpty();
  }
  return ServerProcessor::create(
      nullptr,
      sharded_storage_thread_pool,
      UpdateableSettings<ServerSettings>(server_settings),
      UpdateableSettings<GossipSettings>(gossip_settings),
      config,
      std::make_shared<NoopTraceLogger>(config),
      UpdateableSettings<Settings>(settings),
      stats,
      nullptr,
      make_test_server_plugin_pack(),
      make_test_plugin_registry());
}

std::shared_ptr<ServerProcessor> make_test_server_processor(
    const Settings& settings,
    const ServerSettings& server_settings,
    std::shared_ptr<UpdateableConfig> config,
    ShardedStorageThreadPool* sharded_storage_thread_pool,
    StatsHolder* stats) {
  GossipSettings gossip_settings(create_default_settings<GossipSettings>());
  gossip_settings.enabled = false;
  return make_test_server_processor(settings,
                                    server_settings,
                                    gossip_settings,
                                    std::move(config),
                                    sharded_storage_thread_pool,
                                    stats);
}

void shutdown_test_server(std::shared_ptr<ServerProcessor>& processor) {
  std::unique_ptr<AdminServer> admin_handle;
  std::unique_ptr<EventLoopHandle> connection_listener;
  std::unique_ptr<EventLoopHandle> command_listener;
  std::unique_ptr<EventLoopHandle> gossip_listener;
  std::unique_ptr<EventLoopHandle> ssl_connection_listener;
  std::unique_ptr<LogStoreMonitor> logstore_monitor;
  std::unique_ptr<ShardedStorageThreadPool> storage_thread_pool;
  std::unique_ptr<ShardedRocksDBLocalLogStore> sharded_store;
  std::shared_ptr<SequencerPlacement> sequencer_placement;
  std::unique_ptr<RebuildingCoordinator> rebuilding_coordinator;
  std::unique_ptr<RebuildingSupervisor> rebuilding_supervisor;
  std::shared_ptr<UnreleasedRecordDetector> unreleased_record_detector;

  shutdown_server(admin_handle,
                  connection_listener,
                  command_listener,
                  gossip_listener,
                  ssl_connection_listener,
                  logstore_monitor,
                  processor,
                  storage_thread_pool,
                  sharded_store,
                  sequencer_placement,
                  rebuilding_coordinator,
                  rebuilding_supervisor,
                  unreleased_record_detector,
                  false);
}
}} // namespace facebook::logdevice
