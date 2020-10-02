/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RequestExecutor.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace fb303 {
class FacebookBase2;
}} // namespace facebook::fb303

namespace facebook { namespace logdevice {

/**
 * @file `ThriftServerFactory` will be used to create an `ThriftServer`
 * instance.
 */

class LogDeviceThriftServer;

class ThriftServerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::THRIFT_SERVER_FACTORY;
  }

  /**
   * Creates a new Thrift server instance serving API requests. If there is no
   * implementation available, the admin server will not be started.
   */
  virtual std::unique_ptr<LogDeviceThriftServer>
  operator()(const std::string& name,
             const Sockaddr& listen_addr,
             std::shared_ptr<fb303::FacebookBase2> handler,
             RequestExecutor request_executor) = 0;
};

}} // namespace facebook::logdevice
