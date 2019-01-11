/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "logdevice/include/debug.h"

namespace facebook { namespace logdevice {

/**
 * Represents a module (compilation unit).
 * Used to implement selective logging.
 */
class Module {
 public:
  explicit Module(std::string name)
      : name_(name), log_level_(dbg::Level::NONE) {}

  dbg::Level resetLogLevel() {
    return log_level_.exchange(dbg::Level::NONE);
  }

  dbg::Level setLogLevel(dbg::Level level) {
    return log_level_.exchange(level);
  }

  dbg::Level getLogLevel() const {
    dbg::Level level = log_level_.load();
    return (level == dbg::Level::NONE) ? dbg::currentLevel.load() : level;
  }

  const std::string& getName() const {
    return name_;
  }

 private:
  const std::string name_;
  std::atomic<dbg::Level> log_level_;
};

/**
 * Maintains the map of modules
 */
class ModuleRegistry final {
 public:
  static ModuleRegistry& instance();

  /*
   * Returns module name based on filename
   */
  static std::string moduleNameFromFilename(const char* filename);

  ModuleRegistry();
  ~ModuleRegistry();

  /*
   * Inserts a module with given name if not present
   * Returns module from map
   */
  Module* createOrGet(const std::string& name);

  /*
   * Executes given callback on all modules in the map, with the mutex locked.
   * `visitor` must not do any calls to ModuleRegistry or they will deadlock
   */
  void applyToAll(const std::function<void(Module&)>& visitor);

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Module>> map_;
};

}} // namespace facebook::logdevice
