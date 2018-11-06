/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

class ZookeeperClientBase;

namespace configuration {
class ZookeeperConfig;
} // namespace configuration

class ZookeeperClientFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::ZOOKEEPER_CLIENT_FACTORY;
  }

  virtual std::unique_ptr<ZookeeperClientBase>
  getClient(const configuration::ZookeeperConfig& config) = 0;
};

}} // namespace facebook::logdevice
