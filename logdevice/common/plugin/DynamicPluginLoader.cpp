/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/DynamicPluginLoader.h"

#include <dlfcn.h>

namespace facebook { namespace logdevice {

static const std::string kSharedLibExt = ".so";
constexpr const char* kLogDevicePluginSymbolName = "logdevice_plugin";

PluginVector DynamicPluginLoader::getPlugins() {
  auto maybe_env = readEnv();
  if (!maybe_env.hasValue()) {
    return {};
  }
  // Parse the env variable to extract paths out of it
  auto env_paths = pathsFromEnv(maybe_env.value());

  // Flatten directory paths
  std::vector<boost::filesystem::path> plugin_paths;
  for (auto& path : env_paths) {
    boost::system::error_code err;
    auto is_dir = boost::filesystem::is_directory(path, err);
    if (err) {
      // We can't log errors because the logger might not have been initialized
      // yet, so let's just throw an exception on failures.
      throw std::runtime_error(folly::sformat(
          "Failed to load '{}': {}", path.string(), err.message()));
    }
    if (is_dir) {
      loadFromDir(path, plugin_paths);
    } else {
      plugin_paths.push_back(std::move(path));
    }
  }

  // Load the plugins
  PluginVector plugins;
  for (const auto& path : plugin_paths) {
    // RTLD_LOCAL here means : "Symbols defined in this library are not made
    // available to resolve references in subsequently loaded libraries."
    void* handle = dlopen(path.c_str(), RTLD_LOCAL | RTLD_NOW);
    if (handle == nullptr) {
      throw std::runtime_error(folly::sformat(
          "Failed to dlopen '{}': {}", path.string(), dlerror()));
    }
    void* ptr = dlsym(handle, kLogDevicePluginSymbolName);
    if (ptr == nullptr) {
      continue;
    }
    auto fnptr = reinterpret_cast<Plugin* (*)()>(ptr);
    std::unique_ptr<Plugin> plugin_ptr(fnptr());
    plugins.push_back(std::move(plugin_ptr));
  }

  return plugins;
}

folly::Optional<std::string> DynamicPluginLoader::readEnv() const {
  auto env_value =
      std::getenv(kLogDeviceLoadPluginsEnvVariable().toString().c_str());
  if (env_value == nullptr || strlen(env_value) == 0) {
    return folly::none;
  }
  return std::string(env_value);
}

std::vector<boost::filesystem::path>
DynamicPluginLoader::pathsFromEnv(const std::string& env_value) const {
  std::vector<std::string> values;
  folly::split(":", env_value, values);

  std::vector<boost::filesystem::path> paths;
  paths.reserve(values.size());
  for (auto& value : values) {
    paths.emplace_back(std::move(value));
  }
  return paths;
}

void DynamicPluginLoader::loadFromDir(
    const boost::filesystem::path& dir_path,
    std::vector<boost::filesystem::path>& output_paths) const {
  for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    if (!boost::filesystem::is_directory(it.path()) &&
        it.path().extension() == kSharedLibExt) {
      output_paths.push_back(it.path());
    }
  }
}

}} // namespace facebook::logdevice
