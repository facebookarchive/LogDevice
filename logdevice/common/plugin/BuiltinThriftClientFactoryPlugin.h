/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/ThriftClientFactoryPlugin.h"
#include "logdevice/common/thrift/SimpleThriftClientFactory.h"

using apache::thrift::RocketClientChannel;
using facebook::logdevice::detail::RocketChannelWrapper;
using folly::AsyncSocket;

namespace facebook { namespace logdevice {

/**
 * Basic implementation of Thrift client factory. It uses IO thread pool
 * shared between all clients it creates and creates a new connection for each
 * new client.
 */
class BuiltinThriftClientFactoryPlugin : public ThriftClientFactoryPlugin {
 public:
  // Plugin identifier
  std::string identifier() const override {
    return PluginRegistry::kBuiltin().str();
  }

  // Plugin display name
  std::string displayName() const override {
    return "built-in";
  }

  std::unique_ptr<ThriftClientFactory>
  operator()(const std::shared_ptr<UpdateableConfig>&) override {
    return std::make_unique<SimpleThriftClientFactory>();
  }
};
}} // namespace facebook::logdevice
