/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"

namespace facebook { namespace logdevice {

/**
 * @file `ZookeeperClientFactory` implementation that gives out
 * shared Zookeeper client.
 */

class BuiltinZookeeperClientFactory : public ZookeeperClientFactory {
 public:
  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  std::unique_ptr<ZookeeperClientBase>
  getClient(const configuration::ZookeeperConfig& config) override;
};

}} // namespace facebook::logdevice
