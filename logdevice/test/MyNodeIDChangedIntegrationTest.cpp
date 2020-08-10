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

TEST(MyNodeInfoChangedIntegrationTest, TestShutdown) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .create(4);
  cluster->waitUntilAllAvailable();

  auto config = cluster->getConfig()->get();
  auto nodes_config = config->getNodesConfiguration();

  // Assert that initially all the nodes are up.
  ASSERT_TRUE(cluster->getNode(0).isRunning());
  ASSERT_TRUE(cluster->getNode(1).isRunning());
  ASSERT_TRUE(cluster->getNode(2).isRunning());
  ASSERT_TRUE(cluster->getNode(3).isRunning());

  {
    // Change version of N2
    auto svc_disc = nodes_config->getNodeServiceDiscovery(2);
    ld_check(svc_disc);

    auto new_disc = *svc_disc;
    new_disc.version++;

    nodes_config = nodes_config->applyUpdate(
        NodesConfigurationTestUtil::setNodeAttributesUpdate(
            2, std::move(new_disc), folly::none, folly::none));
    cluster->updateNodesConfiguration(*nodes_config);
  }

  // Expect N2 to gracefull shutdown
  EXPECT_TRUE(cluster->getNode(0).isRunning());
  EXPECT_TRUE(cluster->getNode(1).isRunning());
  EXPECT_EQ(0, cluster->getNode(2).waitUntilExited());
  EXPECT_TRUE(cluster->getNode(3).isRunning());
}
