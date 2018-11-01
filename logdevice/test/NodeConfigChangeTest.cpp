/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class NodeConfigChangeTest : public IntegrationTestBase {};

// Checks if the open connections to old_host have been closed on the node for
// given index.
bool checkIfNodesUpdated(
    std::unique_ptr<IntegrationTestUtils::Cluster>& cluster,
    node_index_t index,
    std::string old_host) {
  auto cb = [&]() {
    auto reply = cluster->getNode(index).socketInfo();
    for (auto const& value : reply) {
      if (value.at("Name") == old_host && value.at("State") == "A")
        return false;
    }
    return true;
  };

  return (wait_until("host disconnect from previous address", cb) == 0);
}

void waitForSocketActivation(
    std::unique_ptr<IntegrationTestUtils::Cluster>& cluster,
    node_index_t index) {
  auto cb = [&]() {
    auto reply = cluster->getNode(index).socketInfo();
    for (auto const& value : reply) {
      if (value.at("State") != "A")
        return false;
    }
    return true;
  };

  wait_until("sockets becomes active", cb);
}

/*
 * This test verfies if changes to the IP of a node in the logdevice config
 * file result in closing of any open sockets to that node */
TEST_F(NodeConfigChangeTest, ChangeDataIP) {
  std::string old_host;

  // Spawn a cluster
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  // Create a client
  std::shared_ptr<Client> client = cluster->createClient();

  // Wait for all sockets on node 0 to become active
  waitForSocketActivation(cluster, 0);

  // Get the current config
  std::shared_ptr<Configuration> current_config = cluster->getConfig()->get();

  // Get the nodes in the server config
  Configuration::Nodes nodes = current_config->serverConfig()->getNodes();
  old_host = nodes[1].address.toString();

  // modify the data address for node 1
  std::string addr = "/nuked";
  nodes[1].address = Sockaddr(addr);

  Configuration::NodesConfig nodes_config(std::move(nodes));
  // create a new config with the modified node config
  Configuration config(current_config->serverConfig()->withNodes(nodes_config),
                       current_config->logsConfig());
  // Write it out to the logdevice.conf file
  cluster->writeConfig(config);

  // Now check the socket info of node 0, to verify that
  // it is not connected with the old host anymore
  ASSERT_EQ(checkIfNodesUpdated(cluster, 0, old_host), true);
}
