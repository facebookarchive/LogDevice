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

/**
 * @file `AdminServerFactory` will be used to create an `AdminServer`
 * instance.
 */

class AdminServer;
class Server;

class AdminServerFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::ADMIN_SERVER_FACTORY;
  }

  /**
   * Creates a new AdminServer instance that will be managed by the server. If
   * there is no implementation available, the admin server will not be started.
   */
  virtual std::unique_ptr<AdminServer> operator()(Server* server) = 0;
};

}} // namespace facebook::logdevice
