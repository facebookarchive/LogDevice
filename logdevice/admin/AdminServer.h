/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/ServerSettings.h"

namespace apache { namespace thrift {
class ThriftServer;
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

class AdminAPIHandler;
class Processor;
class SettingsUpdater;
class ShardedRocksDBLocalLogStore;
class StatsHolder;

/**
 * An interface that will be overridden by plugins to implement an Admin API
 * interface for logdevice.
 */
class AdminServer {
 public:
  /**
   * The address defines the information needed to create a listening
   * socket for the admin server.
   */
  AdminServer(Processor* processor,
              std::shared_ptr<SettingsUpdater> settings_updater,
              UpdateableSettings<ServerSettings> server_settings,
              UpdateableSettings<AdminServerSettings> admin_server_settings,
              StatsHolder* stats_holder);
  /**
   * will be called on server startup, the server startup will fail if this
   * returned false.
   */
  virtual bool start() = 0;
  /**
   * This should stop the admin server and all associated threads. This should
   * be a blocking call that waits until pending work has been processed and
   * all threads have exited.
   */
  virtual void stop() = 0;
  virtual ~AdminServer() {}

  virtual void
  setShardedRocksDBStore(ShardedRocksDBLocalLogStore* sharded_store);

  virtual void setMaintenanceManager(maintenance::MaintenanceManager* mm);

  virtual std::shared_ptr<SafetyChecker> getSafetyChecker();

 protected:
  UpdateableSettings<AdminServerSettings> admin_server_settings_;
  std::shared_ptr<AdminAPIHandler> admin_api_handler_;
  std::shared_ptr<apache::thrift::ThriftServer> server_;
};
}} // namespace facebook::logdevice
