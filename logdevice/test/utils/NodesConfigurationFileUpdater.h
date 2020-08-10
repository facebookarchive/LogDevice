/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

/**
 * A class responsible for reading NodesConfiguration from file and
 * refreshing it periodically when it changes. This is only used in
 * integration tests where the NCM machinery is not there.
 */
class NodesConfigurationFileUpdater {
 public:
  NodesConfigurationFileUpdater(
      std::shared_ptr<UpdateableNodesConfiguration> updateable_nodes_config,
      std::unique_ptr<configuration::nodes::NodesConfigurationStore> store);
  ~NodesConfigurationFileUpdater();

  void start();

 private:
  void pollingLoop();

  std::atomic<bool> shutdown_signaled{false};
  std::thread polling_thread_;
  std::shared_ptr<UpdateableNodesConfiguration> updateable_nodes_config_;
  std::unique_ptr<configuration::nodes::NodesConfigurationStore> store_;
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
