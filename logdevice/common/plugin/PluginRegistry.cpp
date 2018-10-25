/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/plugin/PluginRegistry.h"

#include <folly/dynamic.h>
#include <folly/json.h>

namespace facebook { namespace logdevice {

PluginRegistry::PluginRegistry(PluginVector initial_plugins) {
  loadPlugins(std::move(initial_plugins));
  setCurrentSinglePlugins();
}

std::shared_ptr<Plugin> PluginRegistry::getSinglePluginImpl(PluginType type) {
  auto idx = static_cast<plugin_type_t>(type);
  ld_check(idx < currentSinglePlugins_.size());
  return currentSinglePlugins_[idx].get();
}

std::vector<std::shared_ptr<Plugin>>
PluginRegistry::getMultiPluginImpl(PluginType type) {
  auto idx = static_cast<plugin_type_t>(type);
  auto all_plugins = allPlugins_.rlock();
  ld_check(idx < all_plugins->size());
  const auto& map = (*all_plugins)[idx];
  std::vector<std::shared_ptr<Plugin>> res;
  for (auto& kv : map) {
    ld_check(kv.second);
    res.push_back(kv.second);
  }
  return res;
}

void PluginRegistry::loadPlugins(PluginVector initial_plugins) {
  std::deque<std::unique_ptr<Plugin>> queue;
  auto add_to_queue = [&queue](PluginVector&& v) {
    queue.insert(queue.end(),
                 std::make_move_iterator(v.begin()),
                 std::make_move_iterator(v.end()));
  };
  add_to_queue(std::move(initial_plugins));

  auto all_plugins = allPlugins_.wlock();
  while (!queue.empty()) {
    auto p = std::move(queue.front());
    queue.pop_front();
    ld_check(p);
    PluginType type = p->type();
    std::string identifier = p->identifier();
    auto idx = static_cast<plugin_type_t>(type);
    ld_check(idx < all_plugins->size());
    auto& map = (*all_plugins)[idx];
    if (map.find(identifier) != map.end()) {
      std::string error_str = "Attempting to load plugin with type " +
          toString(type) + " and identifier '" + identifier +
          "', but a plugin with this type and identifier is already loaded";
      throw std::runtime_error(error_str);
    }

    if (type == PluginType::PLUGIN_PROVIDER) {
      // If other plugin providers are encountered among the ones returned by
      // this provider, they are pushed onto the queue for being processed later
      auto provider = dynamic_cast<PluginProvider*>(p.get());
      if (!provider) {
        std::string error_str = "Attempting to load plugin with type " +
            toString(type) + " and identifier '" + identifier +
            "', but unable to dynamic_cast it to PluginProvider";
        throw std::runtime_error(error_str);
      }
      add_to_queue(provider->getPlugins());
    }

    auto res_pair =
        map.insert(std::make_pair(std::move(identifier), std::move(p)));
    ld_check(res_pair.second);
  }
}

void PluginRegistry::setCurrentSinglePlugins() {
  // TODO: pick plugins based on configuration. For now this just picks the
  // only plugin available or the only one that's not built-in if there is
  // a built-in and a loaded one
  auto all_plugins = allPlugins_.rlock();
  for (size_t i = 0; i < all_plugins->size(); ++i) {
    if (pluginTypeDescriptors()[i].allow_multiple) {
      continue;
    }
    auto& map = (*all_plugins)[i];
    if (map.size() == 0) {
      currentSinglePlugins_[i].update(nullptr);
    } else if (map.size() == 1) {
      currentSinglePlugins_[i].update(map.begin()->second);
    } else {
      ld_check(map.size() == 2);
      bool updated = false;
      for (auto& p : map) {
        if (p.first == kBuiltin()) {
          continue;
        }
        ld_check(!updated);
        currentSinglePlugins_[i].update(p.second);
        updated = true;
      }
    }
  }
}

folly::dynamic PluginRegistry::getStateDescription() {
  folly::dynamic res = folly::dynamic::object;

  for (PluginType type : pluginTypeDescriptors().allValidKeys()) {
    if (pluginTypeDescriptors()[type].allow_multiple) {
      auto plugins = getMultiPluginImpl(type);
      if (plugins.size() > 0) {
        folly::dynamic arr = folly::dynamic::array;
        for (auto& p : plugins) {
          ld_check(p);
          arr.push_back(p->displayName());
        }
        res[toString(type)] = std::move(arr);
      }
    } else {
      auto plugin = getSinglePluginImpl(type);
      if (plugin) {
        res[toString(type)] = plugin->displayName();
      }
    }
  }
  return res;
}

std::string PluginRegistry::getStateDescriptionStr() {
  auto data = getStateDescription();
  folly::json::serialization_opts opts;
  return folly::json::serialize(data, opts);
}

void PluginRegistry::addOptions(SettingsUpdater* updater) {
  auto all_plugins = allPlugins_.rlock();
  for (PluginType type : pluginTypeDescriptors().allValidKeys()) {
    auto idx = static_cast<plugin_type_t>(type);
    ld_check(idx < all_plugins->size());
    const auto& map = (*all_plugins)[idx];
    for (auto& kv : map) {
      ld_check(kv.second);
      kv.second->addOptions(updater);
    }
  }
}

}} // namespace facebook::logdevice
