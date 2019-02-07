/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <vector>

#include "logdevice/common/plugin/ConfigSourceFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file `ConfigSourceFactory` implementation that creates `FileConfigSource`,
 * `ZookeeperConfigSource` and `ServerConfigSource`.
 */

class BuiltinConfigSourceFactory : public ConfigSourceFactory {
 public:
  struct Settings : public SettingsBundle {
    const char* getName() const override {
      return "ConfigSourceSettings";
    }
    void defineSettings(SettingEasyInit& init) override;

    // How often to poll for config changes when the config is stored in a local
    // file
    std::chrono::milliseconds file_config_update_interval;

    // How often to poll for config changes when the config is stored in
    // ZooKeeper
    std::chrono::milliseconds zk_config_polling_interval;
  };

  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  std::vector<std::unique_ptr<ConfigSource>>
  operator()(std::shared_ptr<PluginRegistry> plugin_registry) override;
  void addOptions(SettingsUpdater* updater) override;

 private:
  UpdateableSettings<Settings> settings_;
};

}} // namespace facebook::logdevice
