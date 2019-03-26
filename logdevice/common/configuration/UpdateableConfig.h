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
 * UpdateableConfiguration is a proxy class for independent UpdateableConfigs.
 *
 * This class is more or less a wrapper and does not provide atomic reads or
 * updates across multiple configs.
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
    auto zookeeper_config = updateable_zookeeper_config_->get();
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
    return updateable_zookeeper_config_->get();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return updateable_nodes_configuration_->get();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromNCMSource() const {
    return updateable_ncm_nodes_configuration_->get();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfigurationFromServerConfigSource() const {
    return getServerConfig()->getNodesConfigurationFromServerConfigSource();
  }

  std::shared_ptr<configuration::LocalLogsConfig> getLocalLogsConfig() const;
  std::shared_ptr<UpdateableServerConfig> updateableServerConfig() const {
    return updateable_server_config_;
  }

  std::shared_ptr<UpdateableLogsConfig> updateableLogsConfig() const {
    return updateable_logs_config_;
  }

  std::shared_ptr<UpdateableZookeeperConfig> updateableZookeeperConfig() const {
    return updateable_zookeeper_config_;
  }

  std::shared_ptr<UpdateableNodesConfiguration>
  updateableNodesConfiguration() const {
    return updateable_nodes_configuration_;
  }

  std::shared_ptr<UpdateableNodesConfiguration>
  updateableNCMNodesConfiguration() const {
    return updateable_ncm_nodes_configuration_;
  }

  static std::shared_ptr<UpdateableConfig> createEmpty();

 private:
  std::shared_ptr<UpdateableServerConfig> updateable_server_config_;
  std::shared_ptr<UpdateableLogsConfig> updateable_logs_config_;
  std::shared_ptr<UpdateableZookeeperConfig> updateable_zookeeper_config_;

  // This can either be the ServerConfig  NC or the NodesConfigurationManager
  // NC. This updateable is managed by the NodesConfigurationPublisher. Read the
  // comment on top of NodesConfigurationPublisher to understand how
  // and when it is updated. Default to using this updateable NC unless you
  // explicitly need the NCM NC, in which case use
  // updateable_ncm_nodes_configuration_ instead.
  std::shared_ptr<UpdateableNodesConfiguration> updateable_nodes_configuration_;

  // Populated by the NodesConfigurationManager when the NodesConfiguration in
  // the NodesConfigurationStore changes.
  // Use this NC if you explicitly need the NCM NC, otherwise use
  // updateable_nodes_configuration_.
  std::shared_ptr<UpdateableNodesConfiguration>
      updateable_ncm_nodes_configuration_;

  ConfigSubscriptionHandle server_config_subscription_;
  ConfigSubscriptionHandle logs_config_subscription_;
  ConfigSubscriptionHandle zookeeper_config_subscription_;
};
}} // namespace facebook::logdevice
