/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Processor.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPI.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

/**
 * Base class for all handlers processing requests in LogDevice Thrift API
 * server.
 */
class ThriftApiHandlerBase : public virtual thrift::LogDeviceAPISvIf {
 public:
  virtual ~ThriftApiHandlerBase() {}

 protected:
  ThriftApiHandlerBase() = default;

  ThriftApiHandlerBase(
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> updateable_server_settings,
      StatsHolder* stats_holder)
      : processor_(processor),
        settings_updater_(std::move(settings_updater)),
        updateable_server_settings_(updateable_server_settings),
        stats_holder_(stats_holder) {}

  Processor* processor_;
  std::shared_ptr<SettingsUpdater> settings_updater_;
  UpdateableSettings<ServerSettings> updateable_server_settings_;
  StatsHolder* stats_holder_{nullptr};
};

}} // namespace facebook::logdevice
