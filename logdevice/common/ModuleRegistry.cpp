/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ModuleRegistry.h"

#include <functional>

#include <folly/Singleton.h>

namespace facebook { namespace logdevice {

namespace {

struct PrivateTag {};
static folly::LeakySingleton<ModuleRegistry, PrivateTag> the_registry;

} // namespace

ModuleRegistry& ModuleRegistry::instance() {
  return the_registry.get();
}

// extract the module name from filename
// TODO: implement better logic here (eg: strip extension) to use same module
// for .h and .cpp, as well as -inl.h
std::string ModuleRegistry::moduleNameFromFilename(const char* filename) {
  const char* start = nullptr;
  if ((start = strrchr(filename, '/'))) {
    start++;
  } else {
    start = filename;
  }
  return std::string(start);
}

Module* ModuleRegistry::createOrGet(const std::string& name) {
  std::lock_guard<std::mutex> lock(mutex_);

  // First try a lookup to avoid memory allocation in the common case
  auto it = map_.find(name);
  if (it != map_.end()) {
    Module* mod = it->second.get();

    // Note that we can't use ld_check() and the like because they use
    // ModuleRegistry.
    if (!mod) {
      assert(false);
      std::abort();
    }
    assert(name == mod->getName());

    return mod;
  }

  auto result =
      map_.insert(std::make_pair(name, std::make_unique<Module>(name)));
  return result.first->second.get();
}

void ModuleRegistry::applyToAll(const std::function<void(Module&)>& visitor) {
  auto callback = [&visitor](auto& item) { visitor(*item.second); };
  std::unique_lock<std::mutex> lock(mutex_);
  std::for_each(map_.begin(), map_.end(), callback);
}

ModuleRegistry::ModuleRegistry() = default;
ModuleRegistry::~ModuleRegistry() = default;

}} // namespace facebook::logdevice
