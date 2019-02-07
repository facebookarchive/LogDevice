/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConfigInit.h"

#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/ServerConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

int ConfigInit::attach(const std::string& source,
                       std::shared_ptr<PluginRegistry> plugin_registry,
                       std::shared_ptr<UpdateableConfig> updateable_config,
                       std::unique_ptr<LogsConfig> alternative_logs_config,
                       UpdateableSettings<Settings> settings,
                       const ConfigParserOptions& options) {
  int rv;
  auto updater = std::make_shared<TextConfigUpdater>(
      updateable_config, std::move(settings), stats_);

  auto factories = plugin_registry->getMultiPlugin<ConfigSourceFactory>(
      PluginType::CONFIG_SOURCE_FACTORY);
  for (const auto& f : factories) {
    std::vector<std::unique_ptr<ConfigSource>> sources = (*f)(plugin_registry);
    for (auto& src : sources) {
      updater->registerSource(std::move(src));
    }
  }

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
