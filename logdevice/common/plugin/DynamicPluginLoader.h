/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/filesystem.hpp>

#include "logdevice/common/plugin/PluginProvider.h"

namespace facebook { namespace logdevice {

/**
 *
 * Loads dynamic libraries specified by the colon separated LD_LOAD_PLUGINS
 * environment variable. The paths specified here can be files or directories.
 * If its a directory, the plugin loaded will load shared libraries that have
 * the ".so" extension. NOTE: The directory search is not recursive.
 * Those shared libraries are then opened via dlopen and its exported symbols
 * are searched for "logdevice_plugin" symbol. If the plugin doesn't export
 * this symbol, it's ignored.
 *
 */
class DynamicPluginLoader : public PluginProvider {
 public:
  std::string identifier() const override {
    return "dynamic_loader";
  }
  std::string displayName() const override {
    return "Dynamic plugin loader";
  }

  static constexpr folly::StringPiece kLogDeviceLoadPluginsEnvVariable() {
    return folly::StringPiece("LD_LOAD_PLUGINS");
  }

  PluginVector getPlugins() override;

 protected:
  /**
   * Reads the env variable defined by kLogDeviceLoadPluginsEnvVariable. Returns
   * its value if it has one, or folly::none if it's empty or nullptr.
   */
  virtual folly::Optional<std::string> readEnv() const;

 private:
  /**
   * Read the colon separated environment variable value and splits them.
   */
  std::vector<boost::filesystem::path>
  pathsFromEnv(const std::string& env_value) const;

  /**
   * Searches @param dir_path for files ending with ".so" non-recursively and
   * pushes them to @param output_paths.
   */
  void loadFromDir(const boost::filesystem::path& dir_path,
                   std::vector<boost::filesystem::path>& output_paths) const;
};

}} // namespace facebook::logdevice
