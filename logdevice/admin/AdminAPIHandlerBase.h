/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {
class Processor;
class SafetyChecker;
class ServerSettings;
class SettingsUpdater;
class ShardedRocksDBLocalLogStore;
class StatsHolder;

namespace maintenance {
class MaintenanceManager;
}

class AdminAPIHandlerBase : public virtual thrift::AdminAPISvIf {
 public:
  virtual void
  setShardedRocksDBStore(ShardedRocksDBLocalLogStore* sharded_store) {
    sharded_store_ = sharded_store;
  }

  virtual void setMaintenanceManager(maintenance::MaintenanceManager* mm) {
    maintenance_manager_ = mm;
  }

  std::shared_ptr<SafetyChecker> getSafetyChecker() {
    return safety_checker_;
  }

 protected:
  AdminAPIHandlerBase() = default;
  AdminAPIHandlerBase(
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> updateable_server_settings,
      UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
      StatsHolder* stats_holder);

  bool isMaintenanceManagerEnabled() const {
    return updateable_admin_server_settings_->enable_maintenance_manager &&
        maintenance_manager_ != nullptr;
  }

  // throws NotSupported exception if MM is disabled.
  folly::Optional<thrift::NotSupported> failIfMMDisabled() {
    if (!isMaintenanceManagerEnabled()) {
      return thrift::NotSupported("MaintenanceManager is not enabled!");
    }
    return folly::none;
  }

 protected:
  Processor* processor_;
  std::shared_ptr<SettingsUpdater> settings_updater_;
  UpdateableSettings<ServerSettings> updateable_server_settings_;
  UpdateableSettings<AdminServerSettings> updateable_admin_server_settings_;
  StatsHolder* stats_holder_{nullptr};
  std::shared_ptr<SafetyChecker> safety_checker_{nullptr};

 public:
  ShardedRocksDBLocalLogStore* sharded_store_{nullptr};
  maintenance::MaintenanceManager* maintenance_manager_{nullptr};
};
}} // namespace facebook::logdevice
