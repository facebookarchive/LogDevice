/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/dynamic.h>

#include "logdevice/common/plugin/PluginType.h"

namespace facebook { namespace logdevice {

class SettingsUpdater;

/**
 * Interface for pluggable common components of LogDevice.
 *
 * The server and client will keep the Plugin instance alive throughout their
 * lifetime, allowing Plugin subclasses to be stateful.
 *
 * A couple of things to keep in mind when writing plugins:
 * 1) The implementation has to ensure that calls to all methods defined here
 * are thread-safe.
 * 2) PluginRegistry will load all plugins available, but not all of them may
 * end up being used - so avoid consuming a lot of resources or creating
 * threads/doing RPC in the Plugin's constructor. Opt to do things lazily
 * instead.
 */
class Plugin {
 public:
  using Type = PluginType;

  // Returns the type this plugin belongs to. This value should never change
  // throughout the lifetime of the plugin.
  virtual Type type() const = 0;

  // Returns the identifier for this plugin. In the future, this can be used to:
  // 1) load multiple plugins of the same type and pick which one to use
  //    via configuration / settings,
  // 2) have per-plugin configuration.
  // Identifiers should be unique for all loaded plugins of a given type. This
  // value should never change throughout the lifetime of the plugin.
  virtual std::string identifier() const = 0;

  // A string that we can log somewhere when using this plugin. If your plugin
  // is versioned, you can include the version of the plugin in it too.
  virtual std::string displayName() const = 0;

  /**
   * Invoked by the server/client before parsing its command line or config
   * file.  Allows the plugin to define additional groups of options for the
   * parser by maintaining an `UpdateableSettings<T>` instance on its own and
   * passing that instance to `updater->registerSettings()`. Shouldn't store the
   * pointer to SettingsUpdater
   */
  virtual void addOptions(SettingsUpdater* updater) {}

  virtual ~Plugin() {}
};

using PluginVector = std::vector<std::unique_ptr<Plugin>>;

// Helpers for the createPluginVector() function below
template <class T, class... Types>
struct create_plugin_vector_helper {
  static PluginVector create_vector() {
    PluginVector res = create_plugin_vector_helper<Types...>::create_vector();
    res.push_back(std::make_unique<T>());
    return res;
  }
};

template <class T>
struct create_plugin_vector_helper<T> {
  static PluginVector create_vector() {
    PluginVector res;
    res.push_back(std::make_unique<T>());
    return res;
  }
};

// Templated function that constructs a PluginVector with default-constructed
// plugins of specified types
template <class... Types>
PluginVector createPluginVector() {
  PluginVector res = create_plugin_vector_helper<Types...>::create_vector();
  // The helper generates the plug-ins in reverse order, so fixing that here
  std::reverse(res.begin(), res.end());
  return res;
}

}} // namespace facebook::logdevice
