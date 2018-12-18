/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file `AdminServerFactory` will be used to create an `AdminServer`
 * instance.
 */

class AdminServer;
class Processor;
class SettingsUpdater;
class StatsHolder;

class AdminServerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::ADMIN_SERVER_FACTORY;
  }

  /**
   * Creates a new AdminServer instance that will be managed by the server. If
   * there is no implementation available, the admin server will not be started.
   */
  virtual std::unique_ptr<AdminServer> operator()(
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> updateable_server_settings,
      UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
      StatsHolder* stats_holder) = 0;
};

}} // namespace facebook::logdevice
