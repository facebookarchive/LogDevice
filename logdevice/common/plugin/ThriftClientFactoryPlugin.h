/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/thrift/ThriftClientFactory.h"

namespace facebook { namespace logdevice {

class UpdateableConfig;

/**
 * Plugin-backed implementation of Thrift client factory.
 */
class ThriftClientFactoryPlugin : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::THRIFT_CLIENT_FACTORY;
  }

  /**
   * Creates a new factory for obtaining Thrift clients.
   */
  virtual std::unique_ptr<ThriftClientFactory>
  operator()(const std::shared_ptr<UpdateableConfig>&) = 0;
};

}} // namespace facebook::logdevice
