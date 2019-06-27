/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationInit.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

class NodesConfigurationInitIntegrationTest : public IntegrationTestBase {};

std::unique_ptr<Cluster> buildCluster() {
  return IntegrationTestUtils::ClusterFactory{}.create(5);
}

TEST_F(NodesConfigurationInitIntegrationTest, SuccessScenario) {
  auto cluster = buildCluster();

  auto get_protocol_addr = [&](node_index_t idx) {
    return cluster->getNode(idx).addrs_.protocol_addr_.toString();
  };

  {
    // Without client bootstrapping
    // TODO Remove this when client bootstrapping becomes the default.
    auto client = cluster->createIndependentClient();
    auto client_impl = dynamic_cast<ClientImpl*>(client.get());
    auto config = client_impl->getConfig();
    EXPECT_EQ(nullptr, config->getNodesConfigurationFromNCMSource());
  }

  {
    // With NCM client bootstrapping
    auto seed_addr = "data:" + get_protocol_addr(0);
    auto settings = std::unique_ptr<ClientSettings>(ClientSettings::create());
    settings->set("enable-nodes-configuration-manager", "true");
    settings->set("nodes-configuration-seed-servers", seed_addr);

    auto client = cluster->createIndependentClient(
        getDefaultTestTimeout(), std::move(settings));
    auto client_impl = dynamic_cast<ClientImpl*>(client.get());
    auto config = client_impl->getConfig();
    auto nodes_cfg = config->getNodesConfigurationFromNCMSource();
    ASSERT_NE(nullptr, nodes_cfg);
    EXPECT_EQ(get_protocol_addr(0),
              nodes_cfg->getNodeServiceDiscovery(0)->address.toString());
    EXPECT_EQ(get_protocol_addr(4),
              nodes_cfg->getNodeServiceDiscovery(4)->address.toString());
  }
}

TEST_F(NodesConfigurationInitIntegrationTest, SeedDown) {
  auto cluster = buildCluster();
  cluster->shutdownNodes({0});

  auto seed_addr =
      "data:" + cluster->getNode(0).addrs_.protocol_addr_.toString();
  auto settings = std::unique_ptr<ClientSettings>(ClientSettings::create());
  settings->set("enable-nodes-configuration-manager", "true");
  settings->set("nodes-configuration-seed-servers", seed_addr);

  auto client = cluster->createIndependentClient(
      getDefaultTestTimeout(), std::move(settings));
  EXPECT_EQ(nullptr, client);
}
