/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/AdminServer.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/ServerSettings.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"
#include "thrift/lib/cpp2/util/ScopedServerThread.h"

namespace facebook { namespace logdevice {

class Processor;
class SettingsUpdater;
class ShardedRocksDBLocalLogStore;
class StatsHolder;

/**
 * An implementation of `AdminServer` that uses cpp2's ThriftServer.
 */
class SimpleAdminServer : public AdminServer {
 public:
  /**
   * The address defines the information needed to create a listening
   * socket for the admin server.
   */
  SimpleAdminServer(
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> server_settings,
      UpdateableSettings<AdminServerSettings> admin_server_settings,
      StatsHolder* stats_holder);
  /**
   * will be called on server startup, the server startup will fail if this
   * returned false.
   */
  bool start() override;
  /**
   * This should stop the admin server and all associated threads. This should
   * be a blocking call that waits until pending work has been processed and
   * all threads have exited.
   */
  void stop() override;
  virtual ~SimpleAdminServer() {}

 private:
  std::unique_ptr<apache::thrift::util::ScopedServerThread> server_thread_;
  std::atomic_bool started_{false};
};
}} // namespace facebook::logdevice
