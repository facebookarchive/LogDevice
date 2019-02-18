/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/BuiltinConfigSourceFactory.h"

#include "logdevice/common/DataConfigSource.h"
#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/ServerConfigSource.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/ZookeeperConfigSource.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/Validators.h"

using namespace facebook::logdevice::setting_validators;

namespace facebook { namespace logdevice {

std::vector<std::unique_ptr<ConfigSource>> BuiltinConfigSourceFactory::
operator()(std::shared_ptr<PluginRegistry> plugin_registry) {
  std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory =
      plugin_registry->getSinglePlugin<ZookeeperClientFactory>(
          PluginType::ZOOKEEPER_CLIENT_FACTORY);
  std::vector<std::unique_ptr<ConfigSource>> res;
  res.push_back(std::make_unique<FileConfigSource>(settings_));
  res.push_back(std::make_unique<DataConfigSource>());
  res.push_back(std::make_unique<ZookeeperConfigSource>(
      settings_->zk_config_polling_interval,
      std::move(zookeeper_client_factory)));
  res.push_back(std::make_unique<ServerConfigSource>());
  return res;
}

void BuiltinConfigSourceFactory::addOptions(SettingsUpdater* updater) {
  updater->registerSettings(settings_);
}

void BuiltinConfigSourceFactory::Settings::defineSettings(
    SettingEasyInit& init) {
  using namespace SettingFlag;
  init("file-config-update-interval",
       &file_config_update_interval,
       (std::to_string(
            to_msec(FileConfigSource::defaultPollingInterval()).count()) +
        "ms")
           .c_str(),
       validate_positive<ssize_t>(),
       "interval at which to poll config file for changes (if reading config "
       "from file on disk",
       SERVER | CLIENT,
       SettingsCategory::Configuration);
  init("zk-config-polling-interval",
       &zk_config_polling_interval,
       (std::to_string(
            to_msec(ZookeeperConfigSource::defaultPollingInterval()).count()) +
        "ms")
           .c_str(),
       validate_positive<ssize_t>(),
       "polling and retry interval for Zookeeper config source",
       SERVER | CLIENT | CLI_ONLY /* TODO(t13429319): consider changing this*/,
       SettingsCategory::Configuration);
}

}} // namespace facebook::logdevice
