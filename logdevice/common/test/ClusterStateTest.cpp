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
  ASSERT_EQ(cs->getFirstNodeAlive(), 0);

  // config2 will now start from node 1
  std::iota(node_idxs.begin(), node_idxs.end(), 1);
  auto config2 = provisionNodes(node_idxs);
  ASSERT_TRUE(config2->validate());

  cs->updateNodesInConfig(*config2->getServiceDiscovery());
  ASSERT_EQ(cs->getFirstNodeAlive(), 1);
}

}} // namespace facebook::logdevice
