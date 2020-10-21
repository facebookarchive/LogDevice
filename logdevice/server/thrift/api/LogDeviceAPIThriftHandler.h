/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/digest/thrift/DigestThriftHandler.h"
#include "logdevice/server/thrift/LogDeviceThriftHandler.h"
#include "logdevice/server/thrift/api/CompatThriftHandler.h"

namespace facebook { namespace logdevice {

/**
 * Entry point for all incoming requests to LogDevice Thrift API.
 *
 * Unfortunately Thrift does not support mutliple inheritance so we have to have
 * single interface and thus single top-level handler class. In order to make it
 * manageable we split this single handler into multiple, each of them
 * responsible for its own subset of API and later assmble them together into
 * single class using inheritance.
 * The overall (simplified) inheritance graph looks like this:
 *
 *                       LogDeviceAPISvIf
 *                    (generated Thrift API)
 *                                |
 *                      ThriftApiHandlerBase
 *        (to share some pieces of implementation between handlers)
 *                     /          |            \
 *          CompatHandler    OtherHandler    OneMoreHandler
 *                      \         |           /
 *                     LogDeviceAPIThriftHandler
 *            (top level class injected into Thrift server)
 */
class LogDeviceAPIThriftHandler : public LogDeviceThriftHandler,
                                  public CompatThriftHandler,
                                  public server::digest::DigestThriftHandler {
 public:
  LogDeviceAPIThriftHandler(
      const std::string& name,
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> updateable_server_settings,
      StatsHolder* stats_holder)
      : ThriftApiHandlerBase(processor,
                             std::move(settings_updater),
                             std::move(updateable_server_settings),
                             stats_holder),
        LogDeviceThriftHandler(name, processor) {}
};

}} // namespace facebook::logdevice
