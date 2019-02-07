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

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file `ConfigSourceFactory` will be used to create one or more `ConfigSource`
 * instances.
 */

class ConfigSource;
class PluginRegistry;

class ConfigSourceFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::CONFIG_SOURCE_FACTORY;
  }

  /**
   * Allows the plugin to supply ConfigSource instances that will be registered
   * with the TextConfigUpdater.  Invoked by the server/client before fetching
   * the config.
   */
  virtual std::vector<std::unique_ptr<ConfigSource>>
  operator()(std::shared_ptr<PluginRegistry> plugin_registry) = 0;
};

}} // namespace facebook::logdevice
