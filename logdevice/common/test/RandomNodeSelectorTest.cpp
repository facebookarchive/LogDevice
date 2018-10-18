/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/configuration/Configuration.h"

using namespace facebook::logdevice;

TEST(RandomNodeSelector, OneNode) {
  auto node_config = createSimpleNodesConfig(1 /* 1 node */);
  auto server_config = ServerConfig::fromDataTest(
      "random_node_selector_test", std::move(node_config));

  auto nodes = server_config->getNodes();
  auto node_id = NodeID(nodes.begin()->first, nodes.begin()->second.generation);

  EXPECT_EQ(node_id, RandomNodeSelector::getNode(*server_config));
}

TEST(RandomNodeSelector, ExcludeNode) {
  auto node_config = createSimpleNodesConfig(2 /* 2 nodes*/);
  auto server_config = ServerConfig::fromDataTest(
      "random_node_selector_test", std::move(node_config));

  auto nodes = server_config->getNodes();

  auto exclude = NodeID(nodes.begin()->first, nodes.begin()->second.generation);
  auto node =
      NodeID((++nodes.begin())->first, (++nodes.begin())->second.generation);

  EXPECT_EQ(node, RandomNodeSelector::getNode(*server_config, exclude));
}

TEST(RandomNodeSelector, DontExcludeSingleNode) {
  auto node_config = createSimpleNodesConfig(1 /* 1 node */);
  auto server_config = ServerConfig::fromDataTest(
      "random_node_selector_test", std::move(node_config));

  auto nodes = server_config->getNodes();

  // exclude the only node there is
  auto exclude = NodeID(nodes.begin()->first, nodes.begin()->second.generation);
  auto node = exclude;

  EXPECT_EQ(node, RandomNodeSelector::getNode(*server_config, exclude));
}
