/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/NodesConfigurationFileUpdater.h"

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

namespace {
constexpr std::chrono::seconds kPollingInterval{1};
}

NodesConfigurationFileUpdater::NodesConfigurationFileUpdater(
    std::shared_ptr<UpdateableNodesConfiguration> updateable_nodes_config,
    std::unique_ptr<configuration::nodes::NodesConfigurationStore> store)
    : updateable_nodes_config_(std::move(updateable_nodes_config)),
      store_(std::move(store)) {}

NodesConfigurationFileUpdater::~NodesConfigurationFileUpdater() {
  shutdown_signaled = true;
  polling_thread_.join();
}

void NodesConfigurationFileUpdater::start() {
  polling_thread_ =
      std::thread(&NodesConfigurationFileUpdater::pollingLoop, this);
}

void NodesConfigurationFileUpdater::pollingLoop() {
  while (!shutdown_signaled.load()) {
    folly::Optional<membership::MembershipVersion::Type> current_version;
    if (auto current_nc = updateable_nodes_config_->get();
        current_nc != nullptr) {
      current_version = current_nc->getVersion();
    }

    std::string serialized_nc;
    auto status = store_->getConfigSync(&serialized_nc, current_version);

    if (status == Status::OK) {
      auto new_nc = configuration::nodes::NodesConfigurationCodec::deserialize(
          std::move(serialized_nc));
      ld_check(new_nc);
      updateable_nodes_config_->update(std::move(new_nc));
    } else if (status == Status::UPTODATE) {
      // Do nothing
    } else {
      ld_error("Failed reading the nodes configuration from the store: %s",
               error_name(status));
    }

    std::this_thread::sleep_for(kPollingInterval);
  }
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
