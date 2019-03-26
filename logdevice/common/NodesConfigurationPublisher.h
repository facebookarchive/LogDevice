/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

/**
 * This is used to publish a new NodesConfiguration (NC) to the
 * UpdateableNodesConfiguration. The published NC can be either be the
 * ServerConfig based NC or the NCM NC based on the setting:
 * enable_nodes_configuration &&
 * use_nodes_configuration_manager_nodes_configuration.
 *
 * This class subscribes to ServerConfig, settings and NCM NC changes and
 * on change it evaluates whether it needs to publish a new NC to the updateable
 * or not.
 */
class NodesConfigurationPublisher {
 public:
  NodesConfigurationPublisher(std::shared_ptr<UpdateableConfig> config,
                              UpdateableSettings<Settings> settings);

 private:
  void publish();

  std::shared_ptr<UpdateableConfig> config_;
  UpdateableSettings<Settings> settings_;

  // The subscriptions responsible for refreshing the
  // UpdateableNodesConfiguration.
  UpdateableSettings<Settings>::SubscriptionHandle settings_subscription_;
  ConfigSubscriptionHandle server_config_subscription_;
  ConfigSubscriptionHandle ncm_nodes_configuration_subscription_;
};

}} // namespace facebook::logdevice
