/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClusterState.h"

#include <memory>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/membership/StorageMembership.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

using namespace configuration::nodes;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::membership::MembershipVersion;

class ClusterStateTest : public ::testing::Test {
 public:
  std::unique_ptr<ClusterState>
  makeOne(const configuration::nodes::NodesConfiguration& nconfig) {
    return std::make_unique<ClusterState>(
        nconfig.getMaxNodeIndex() + 1, nullptr, *nconfig.getServiceDiscovery());
  }
};

TEST_F(ClusterStateTest, GetFirstNodeShouldLookIntoConfig) {
  using namespace NodesConfigurationTestUtil;
  const ssize_t nnodes = 10;
  std::vector<node_index_t> node_idxs(nnodes);
  std::iota(node_idxs.begin(), node_idxs.end(), 0);

  auto config = provisionNodes(node_idxs);
  ASSERT_TRUE(config->validate());

  auto cs = makeOne(*config);
  ASSERT_EQ(cs->getFirstNodeAlive().value(), 0);

  // config2 will now start from node 1
  std::iota(node_idxs.begin(), node_idxs.end(), 1);
  auto config2 = provisionNodes(node_idxs);
  ASSERT_TRUE(config2->validate());

  cs->updateNodesInConfig(*config2->getServiceDiscovery());
  ASSERT_EQ(cs->getFirstNodeAlive().value(), 1);
}
TEST_F(ClusterStateTest, GetDefaultNodeStateAndStatus) {
  using namespace NodesConfigurationTestUtil;
  const ssize_t nnodes = 3;
  std::vector<node_index_t> node_idxs(nnodes);
  std::iota(node_idxs.begin(), node_idxs.end(), 0);

  auto config = provisionNodes(node_idxs);
  ASSERT_TRUE(config->validate());

  auto cs = makeOne(*config);
  ASSERT_EQ(cs->getFirstNodeAlive().value(), 0);
  ASSERT_EQ(cs->getNodeState(0), ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(cs->getNodeStatus(0), NodeHealthStatus::HEALTHY);
}

TEST_F(ClusterStateTest, GetFirstNodeAliveReturnsNoneIfAllNodesAreDead) {
  using namespace NodesConfigurationTestUtil;
  std::vector<node_index_t> node_idxs(3, 0);

  auto config = provisionNodes(node_idxs);
  ASSERT_TRUE(config->validate());

  // Make a ClusterState instance and set all nodes to DEAD status
  auto cs = makeOne(*config);
  for (auto [node_index, _] : cs->getWholeClusterStatus()) {
    cs->setNodeState(node_index, ClusterState::NodeState::DEAD);
  }

  ASSERT_FALSE(cs->getFirstNodeAlive().has_value());
}

}} // namespace facebook::logdevice
