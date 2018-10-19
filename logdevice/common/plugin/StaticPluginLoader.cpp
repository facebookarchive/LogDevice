/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/StaticPluginLoader.h"

#include <dlfcn.h>

namespace facebook { namespace logdevice {

PluginVector StaticPluginLoader::getPlugins() {
  const std::vector<std::string> symbol_names{
      "logdevice_plugin",
      // legacy names for plugins - new plugins shouldn't use these
      "logdevice_server_plugin",
      "logdevice_client_plugin"};
  PluginVector res;
  for (const auto& symbol : symbol_names) {
    void* ptr = dlsym(RTLD_DEFAULT, symbol.c_str());
    if (ptr == nullptr) {
      continue;
    }
    auto fnptr = reinterpret_cast<Plugin* (*)()>(ptr);
    std::unique_ptr<Plugin> plugin_ptr(fnptr());
    res.push_back(std::move(plugin_ptr));
  }
  return res;
}

}} // namespace facebook::logdevice
