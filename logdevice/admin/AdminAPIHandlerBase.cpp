/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIHandlerBase.h"

#include "logdevice/admin/safety/SafetyChecker.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

AdminAPIHandlerBase::AdminAPIHandlerBase(
    Processor* processor,
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> updateable_server_settings,
    UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
    StatsHolder* stats_holder)
    : processor_(processor),
      settings_updater_(std::move(settings_updater)),
      updateable_server_settings_(updateable_server_settings),
      updateable_admin_server_settings_(updateable_admin_server_settings),
      stats_holder_(stats_holder) {}
}} // namespace facebook::logdevice
