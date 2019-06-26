/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"

#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

namespace {

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::NodesConfigurationTestUtil;

ServiceDiscoveryConfig buildConfig(std::vector<node_index_t> indices) {
  ServiceDiscoveryConfig cfg;

  ServiceDiscoveryConfig::Update update;
  for (auto i : indices) {
    update.addNode(
        i,
        {ServiceDiscoveryConfig::UpdateType::PROVISION,
         std::make_unique<NodeServiceDiscovery>(genDiscovery(i, {3}, ""))});
  }

  ServiceDiscoveryConfig new_cfg;
  cfg.applyUpdate(std::move(update), &new_cfg);
  return new_cfg;
}

TEST(NodeIndicesAllocatorTest, TestAllocate) {
  auto cfg = buildConfig({1, 2, 3, 5, 8});

  NodeIndicesAllocator allocator{};
  EXPECT_EQ((std::deque<node_index_t>{0, 4, 6, 7, 9, 10}),
            allocator.allocate(cfg, 6));

  // Same allocation request returns the same results
  EXPECT_EQ((std::deque<node_index_t>{0, 4, 6, 7, 9, 10}),
            allocator.allocate(cfg, 6));

  EXPECT_EQ((std::deque<node_index_t>{0}), allocator.allocate(cfg, 1));
  EXPECT_EQ((std::deque<node_index_t>{}), allocator.allocate(cfg, 0));
}

TEST(NodeIndicesAllocatorTest, TestAllocateEmptyConfig) {
  auto cfg = buildConfig({});

  NodeIndicesAllocator allocator{};
  EXPECT_EQ((std::deque<node_index_t>{0, 1, 2}), allocator.allocate(cfg, 3));
  EXPECT_EQ((std::deque<node_index_t>{}), allocator.allocate(cfg, 0));
}

} // namespace
