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

class ServerConfigIntegrationTest : public IntegrationTestBase {};

TEST_F(ServerConfigIntegrationTest, NodeIndexChange) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);

  cluster->getNode(0).waitUntilStarted();
  cluster->getNode(1).waitUntilStarted();

  const auto get_stat = [](auto& node) {
    return node.stats()["last_config_valid"];
  };

  EXPECT_EQ(1, get_stat(cluster->getNode(0)));
  EXPECT_EQ(1, get_stat(cluster->getNode(1)));

  auto config = cluster->getConfig()->get();
  auto server_config = config->serverConfig();
  auto nodes = server_config->getNodes();

  // swap index of the two nodes
  configuration::Nodes new_nodes{
      {0, std::move(nodes[1])}, {1, std::move(nodes[0])}};
  cluster->writeConfig(config->serverConfig()
                           ->withNodes(configuration::NodesConfig(new_nodes))
                           .get(),
                       config->logsConfig().get());

  // can't use cluster->waitForConfigUpdate(), because the config is never
  // updated due to it being invalid
  wait_until([&] {
    return get_stat(cluster->getNode(0)) == 0 &&
        get_stat(cluster->getNode(1)) == 0;
  });

  // this is implicitly true due to the wait_until, but let's be explicit
  EXPECT_EQ(0, get_stat(cluster->getNode(0)));
  EXPECT_EQ(0, get_stat(cluster->getNode(1)));
}
