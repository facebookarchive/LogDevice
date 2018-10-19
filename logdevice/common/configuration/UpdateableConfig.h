/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfigTmpl.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

namespace configuration {
class LocalLogsConfig;
}

/**
 * UpdateableConfiguration is a proxy class for two independent
 * UpdateableConfigs (ServerConfig and LogsConfig).
 *
 * The purpose of this is to to offer a shortcut to get a snapshot of the latest
 * known configs in both ServerConfig and LogsConfig. The returned object when
 * calling get() is `Configuration` which holds two shared_ptr to ServerConfig
 * and LogsConfig returned from the two underlying updateables.
 */
class UpdateableConfig : public configuration::UpdateableConfigBase {
 public:
  using NodesConfig = facebook::logdevice::configuration::NodesConfig;

  UpdateableConfig()
      : UpdateableConfig(std::make_shared<UpdateableServerConfig>(),
                         std::make_shared<UpdateableLogsConfig>(),
                         std::make_shared<UpdateableZookeeperConfig>()) {}

  UpdateableConfig(
      std::shared_ptr<UpdateableServerConfig> updateable_server_config,
      std::shared_ptr<UpdateableLogsConfig> updateable_logs_config,
      std::shared_ptr<UpdateableZookeeperConfig> updateable_zookeeper_config);

  // very useful in testing if you want to create an updateable configuration
  // that is wired to specific configuration object
  explicit UpdateableConfig(std::shared_ptr<Configuration> init_config);

  ~UpdateableConfig();

  std::shared_ptr<Configuration> get() const {
    auto server_config = updateable_server_config_->get();
    auto logs_config = updateable_logs_config_->get();
    auto zookeeper_config = updatable_zookeeper_config_->get();
    if (server_config == nullptr) {
      // we don't return configuration unless we have at least a ServerConfig
      return nullptr;
    }
    return std::make_shared<Configuration>(
        server_config, logs_config, zookeeper_config);
  }
  std::shared_ptr<ServerConfig> getServerConfig() const {
    return updateable_server_config_->get();
  }
  std::shared_ptr<LogsConfig> getLogsConfig() const {
    return updateable_logs_config_->get();
  }
  std::shared_ptr<ZookeeperConfig> getZookeeperConfig() const {
    return updatable_zookeeper_config_->get();
  }
  std::shared_ptr<configuration::LocalLogsConfig> getLocalLogsConfig() const;
  std::shared_ptr<UpdateableServerConfig> updateableServerConfig() const {
    return updateable_server_config_;
  }

  std::shared_ptr<UpdateableLogsConfig> updateableLogsConfig() const {
    return updateable_logs_config_;
  }

  std::shared_ptr<UpdateableZookeeperConfig> updateableZookeeperConfig() const {
    return updatable_zookeeper_config_;
  }

  /*
   * [DEPRECATED] This should not be used anymore. This method is only there
   * to make it easy for older code to work, it's recommended to only
   * update the config object that you need and not update both at the same
   * time using this method.
   */
  int updateBaseConfig(const std::shared_ptr<Configuration>& config) {
    return updateable_logs_config_->update(config->logsConfig()) &&
        updateable_server_config_->update(config->serverConfig());
  }

  static std::shared_ptr<UpdateableConfig> createEmpty();

 private:
  std::shared_ptr<UpdateableServerConfig> updateable_server_config_;
  std::shared_ptr<UpdateableLogsConfig> updateable_logs_config_;
  std::shared_ptr<UpdateableZookeeperConfig> updatable_zookeeper_config_;

  ConfigSubscriptionHandle server_config_subscription_;
  ConfigSubscriptionHandle logs_config_subscription_;
  ConfigSubscriptionHandle zookeeper_config_subscription_;
};
}} // namespace facebook::logdevice
