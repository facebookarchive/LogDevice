/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/include/ClientSettings.h"

namespace facebook { namespace logdevice {

class ClientSettingsImpl : public ClientSettings {
 public:
  ClientSettingsImpl();
  using ClientSettings::set; // get all set() overloads from the base class

  int set(const char* name, const char* value);

  /**
   * Exposes the wrapped Settings instance.
   */
  UpdateableSettings<Settings> getSettings() const {
    return settings_;
  }

  /**
   * Returns the SettingsUpdater
   */
  std::shared_ptr<SettingsUpdater> getSettingsUpdater() const {
    return settings_updater_;
  }

  /**
   * Returns the plugin registry if one was created in this instance
   */
  std::shared_ptr<PluginRegistry> getPluginRegistry() const {
    return plugin_registry_;
  }

 private:
  std::shared_ptr<PluginRegistry> plugin_registry_;
  UpdateableSettings<Settings> settings_;
  std::shared_ptr<SettingsUpdater> settings_updater_;
};

}} // namespace facebook::logdevice
