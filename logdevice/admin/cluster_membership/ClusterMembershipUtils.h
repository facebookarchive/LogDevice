/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/if/gen-cpp2/cluster_membership_types.h"
#include "logdevice/common/configuration/nodes/NodeUpdateBuilder.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

// A helper that populates a NodeUpdateBuilder from a NodeConfig thrift
// structure. If the generated update builder fails the validation, a
// ClusterMembershipFailedNode will be returned instead.
folly::Expected<configuration::nodes::NodeUpdateBuilder,
                thrift::ClusterMembershipFailedNode>
nodeUpdateBuilderFromNodeConfig(const logdevice::thrift::NodeConfig& cfg);

thrift::ClusterMembershipFailedNode
buildNodeFailure(node_index_t idx,
                 thrift::ClusterMembershipFailureReason reason,
                 const std::string& message);

thrift::ClusterMembershipFailedNode
buildNodeFailure(thrift::NodeID id,
                 thrift::ClusterMembershipFailureReason reason,
                 const std::string& message);

}}}} // namespace facebook::logdevice::admin::cluster_membership
