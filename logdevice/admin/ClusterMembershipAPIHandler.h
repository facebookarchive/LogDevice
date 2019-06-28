/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * ClusterMembershipAPIHandler is the class responsible for handling cluster
 * membership changes API (add/remove/update node).
 * This class delegates the actual logic of each method to its own handler that
 * then responds with a list of NodesConfiguration updates for this class to
 * apply.
 */
class ClusterMembershipAPIHandler : public virtual AdminAPIHandlerBase {
 public:
  // See admin.thrift for documentation
  folly::SemiFuture<std::unique_ptr<thrift::RemoveNodesResponse>>
  semifuture_removeNodes(
      std::unique_ptr<thrift::RemoveNodesRequest> req) override;

  // See admin.thrift for documentation
  folly::SemiFuture<std::unique_ptr<thrift::AddNodesResponse>>
  semifuture_addNodes(std::unique_ptr<thrift::AddNodesRequest> req) override;

  // See admin.thrift for documentation
  folly::SemiFuture<std::unique_ptr<thrift::UpdateNodesResponse>>
  semifuture_updateNodes(
      std::unique_ptr<thrift::UpdateNodesRequest> req) override;

 private:
  /**
   * Applies the NodesConfiguration update to the NodesConfigurationManager
   * (NCM), and returns a SemiFuture that resolves to the new nodes
   * configuration returned by the NCM after applying the update.
   * If the NCM fails to apply the update, the future will be fullfiled with a
   * NodesConfigurationManagerError exception.
   */
  folly::SemiFuture<std::shared_ptr<const NodesConfiguration>>
  applyNodesConfigurationUpdates(NodesConfiguration::Update updates);
};

}} // namespace facebook::logdevice
