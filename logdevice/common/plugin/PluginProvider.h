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
 * A type of plugin that provides other plugins. These, in turn, can also be
 * PluginProviders. This enables customization of the way plugins are loaded and
 * bundling plugins of various types together.
 */
class PluginProvider : public Plugin {
 public:
  Type type() const override {
    return Type::PLUGIN_PROVIDER;
  }
  // settings are parsed after the plugins are loaded. PluginProviders are only
  // used before that, so they won't see any setting changes
  void addOptions(SettingsUpdater* updater) override final {}
  virtual PluginVector getPlugins() = 0;
};

}} // namespace facebook::logdevice
