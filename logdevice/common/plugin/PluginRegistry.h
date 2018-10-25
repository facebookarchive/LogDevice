/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include <folly/Synchronized.h>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/plugin/PluginProvider.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice {

/**
 * The central repository for all plugins that have been loaded.
 * Loading is only done once, when the registry is being constructed.
 * After that all plugins are available by calling getSinglePlugin() or
 * getMultiPlugin() - depending on whether a given plugin type allows multiple
 * active plugins per type or not.
 */
class PluginRegistry {
 public:
  explicit PluginRegistry(PluginVector initial_plugins);

  template <class T>
  std::shared_ptr<T> getSinglePlugin(Plugin::Type type) {
    ld_check(!pluginTypeDescriptors()[type].allow_multiple);
    std::shared_ptr<Plugin> p = getSinglePluginImpl(type);
    auto res = std::dynamic_pointer_cast<T>(p);
    ld_check(!p || res);
    return res;
  }

  template <class T>
  std::vector<std::shared_ptr<T>> getMultiPlugin(Plugin::Type type) {
    ld_check(pluginTypeDescriptors()[type].allow_multiple);
    std::vector<std::shared_ptr<Plugin>> plugins = getMultiPluginImpl(type);
    std::vector<std::shared_ptr<T>> res;
    res.reserve(plugins.size());
    for (auto& p : plugins) {
      auto cast_ptr = std::dynamic_pointer_cast<T>(p);
      ld_check(cast_ptr);
      res.emplace_back(std::move(cast_ptr));
    }
    return res;
  }

  folly::dynamic getStateDescription();
  std::string getStateDescriptionStr();

  // identifier for built-in plugin implementations
  static constexpr folly::StringPiece kBuiltin() {
    return folly::StringPiece("builtin");
  }

  // Runs addOptions() for all loaded plugins
  void addOptions(SettingsUpdater* updater);

 private:
  void loadPlugins(PluginVector initial_plugins);

  // sets current plugins for plugin types that don't support multiple active
  // plugin implementations
  void setCurrentSinglePlugins();

  std::shared_ptr<Plugin> getSinglePluginImpl(Plugin::Type type);
  std::vector<std::shared_ptr<Plugin>> getMultiPluginImpl(Plugin::Type type);

  using plugin_type_t = std::underlying_type<PluginType>::type;
  static constexpr size_t num_plugin_types_ =
      static_cast<plugin_type_t>(PluginType::MAX);
  using PerTypeMap = std::unordered_map<std::string, std::shared_ptr<Plugin>>;

  // All plugins loaded from everywhere
  folly::Synchronized<std::array<PerTypeMap, num_plugin_types_>> allPlugins_;

  // Currently active plugin for each type where multiple plugins aren't
  // allowed.
  std::array<UpdateableSharedPtr<Plugin>, num_plugin_types_>
      currentSinglePlugins_;
};

}} // namespace facebook::logdevice
