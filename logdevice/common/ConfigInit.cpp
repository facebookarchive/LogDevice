/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ConfigInit.h"

#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/LegacyPluginPack.h"
#include "logdevice/common/ServerConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

int ConfigInit::attach(const std::string& source,
                       std::shared_ptr<LegacyPluginPack> plugin,
                       std::shared_ptr<PluginRegistry> plugin_registry,
                       std::shared_ptr<UpdateableConfig> updateable_config,
                       std::unique_ptr<LogsConfig> alternative_logs_config,
                       UpdateableSettings<Settings> settings,
                       const ConfigParserOptions& options) {
  int rv;
  auto updater = std::make_shared<TextConfigUpdater>(
      updateable_config, std::move(settings), stats_);
  updater->registerSource(
      std::make_unique<FileConfigSource>(file_polling_interval_));
  updater->registerSource(
      std::make_unique<ZookeeperConfigSource>(zk_polling_interval_));
  updater->registerSource(std::make_unique<ServerConfigSource>(
      alternative_logs_config.get(), plugin, plugin_registry));

  // Ask the plugin if it wants to register additional sources
  plugin->registerConfigSources(*updater, zk_polling_interval_);

  rv = updater->load(source, std::move(alternative_logs_config), options);
  if (rv != 0) {
    return rv;
  }
  rv = updater->waitForInitialLoad(timeout_);
  if (rv != 0) {
    return rv;
  }

  updateable_config->updateableServerConfig()->setUpdater(updater);
  updateable_config->updateableLogsConfig()->setUpdater(updater);
  updateable_config->updateableZookeeperConfig()->setUpdater(updater);
  return 0;
}

}} // namespace facebook::logdevice
