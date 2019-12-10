/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#include "logdevice/test/utils/SelfRegisteredCluster.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

TEST(SelfRegistrationIntegrationTest, startNode) {
  auto cluster = IntegrationTestUtils::SelfRegisteredCluster::create();

  // Start a new logdeviced
  auto node = cluster->createSelfRegisteringNode("new_node");
  node->start();
  node->waitUntilStarted();

  wait_until("The new node is registered in the nodes config", [&]() {
    auto nc = cluster->readNodesConfigurationFromStore();
    ld_check(nc);
    auto svd = nc->getServiceDiscovery();
    return !svd->isEmpty();
  });

  node_index_t node_idx;

  {
    // Check that the node registered in the config by search for its name
    auto nc = cluster->readNodesConfigurationFromStore();
    ASSERT_NE(nullptr, nc);
    const NodeServiceDiscovery* node_svd = nullptr;
    for (const auto& [nid, config] : *nc->getServiceDiscovery()) {
      if (config.name == "new_node") {
        node_svd = &config;
        node_idx = nid;
      }
    }
    ASSERT_NE(nullptr, node_svd);
  }

  // After provisioning the node should mark all of its shards as provisioned
  wait_until("The new node marks itself as provisioned", [&]() {
    auto nc = cluster->readNodesConfigurationFromStore();
    ld_check(nc);
    auto mem = nc->getStorageMembership();
    auto shard_states = mem->getShardStates(node_idx);

    auto all_provisioned = true;
    for (const auto& [shard, state] : shard_states) {
      ld_info("%s", state.toString().c_str());
      all_provisioned = all_provisioned &&
          state.storage_state == membership::StorageState::NONE;
    }
    ld_info("");
    return all_provisioned;
  });
}
