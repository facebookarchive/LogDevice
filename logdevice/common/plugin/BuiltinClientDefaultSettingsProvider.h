/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Portability.h>

#include "logdevice/common/plugin/DefaultSettingsProvider.h"
#include "logdevice/common/plugin/PluginRegistry.h"

namespace facebook { namespace logdevice {

/**
 * @file Provides default settings overrides for clients
 */

class BuiltinClientDefaultSettingsProvider : public DefaultSettingsProvider {
 public:
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }
  std::string displayName() const override {
    return "built-in";
  }

  virtual std::unordered_map<std::string, std::string>
  getDefaultSettingsOverride() const override {
    // By default be paranoid and don't crash the process on failed assert.
    return {{"abort-on-failed-check", folly::kIsDebug ? "true" : "false"}};
  }
};

}} // namespace facebook::logdevice
