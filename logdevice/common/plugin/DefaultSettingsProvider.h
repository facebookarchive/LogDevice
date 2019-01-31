/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file Provides default settings. Used to override default values for some
 * settings depending on the context where the settings are loaded.
 */

class DefaultSettingsProvider : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::DEFAULT_SETTINGS_PROVIDER;
  }

  virtual std::unordered_map<std::string, std::string>
  getDefaultSettingsOverride() const = 0;
};

}} // namespace facebook::logdevice
