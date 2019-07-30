/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

TEST(MyNodeIDChangedIntegrationTest, TestShutdown) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .create(4);
  cluster->waitUntilAllAvailable();

  auto config = cluster->getConfig()->get();
  auto server_config = config->serverConfig();
  auto nodes = server_config->getNodes();

  // Assert that initially all the nodes are up.
  ASSERT_TRUE(cluster->getNode(0).isRunning());
  ASSERT_TRUE(cluster->getNode(1).isRunning());
  ASSERT_TRUE(cluster->getNode(2).isRunning());
  ASSERT_TRUE(cluster->getNode(3).isRunning());

  // swap index of N0, N1 and change generation of N2
  nodes[2].generation++;
  configuration::Nodes new_nodes{{0, std::move(nodes[1])},
                                 {1, std::move(nodes[0])},
                                 {2, std::move(nodes[2])},
                                 {3, std::move(nodes[3])}};
  cluster->writeConfig(config->serverConfig()
                           ->withNodes(configuration::NodesConfig(new_nodes))
                           .get(),
                       nullptr,
                       false);

  // Expect N0 & N1 to gracefull shutdown, N2 shouldn't get impacted.
  EXPECT_EQ(0, cluster->getNode(0).waitUntilExited());
  EXPECT_EQ(0, cluster->getNode(1).waitUntilExited());
  EXPECT_EQ(0, cluster->getNode(2).waitUntilExited());
  EXPECT_TRUE(cluster->getNode(3).isRunning());
}
