/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::thrift;

class AdminAPINodeConfigTest : public IntegrationTestBase {};

TEST_F(AdminAPINodeConfigTest, getNodeConfig) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setNumRacks(2)
                     .create(3);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(1).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  // Find All
  NodesConfigResponse response1;
  NodesFilter noop_filter;
  admin_client->sync_getNodesConfig(response1, noop_filter);
  ASSERT_EQ(3, response1.get_nodes().size());

  // Find by Index
  NodesConfigResponse response2;
  NodesFilter index_filter;
  thrift::NodeID node_identifier;
  node_identifier.set_node_index(0);
  index_filter.set_node(node_identifier);
  admin_client->sync_getNodesConfig(response2, index_filter);
  auto resp_nodes = response2.get_nodes();
  ASSERT_EQ(1, response2.get_version());
  ASSERT_EQ(1, resp_nodes.size());
  auto node1 = resp_nodes[0];
  ASSERT_EQ(0, node1.get_node_index());
  ASSERT_TRUE(node1.get_location());
  ASSERT_EQ("rg1.dc1.cl1.rw1.rk1", *node1.get_location());

  ASSERT_TRUE(node1.get_location_per_scope().size() > 0);
  thrift::Location expected_location({
      {thrift::LocationScope::REGION, "rg1"},
      {thrift::LocationScope::DATA_CENTER, "dc1"},
      {thrift::LocationScope::CLUSTER, "cl1"},
      {thrift::LocationScope::ROW, "rw1"},
      {thrift::LocationScope::RACK, "rk1"},
  });
  ASSERT_EQ(expected_location, node1.get_location_per_scope());

  auto data_address = node1.get_data_address();
  ASSERT_EQ(SocketAddressFamily::UNIX, data_address.get_address_family());
  ASSERT_TRUE(data_address.get_address());
  ASSERT_FALSE(data_address.get_port());
  std::set<Role> expected_roles = {Role::SEQUENCER, Role::STORAGE};
  ASSERT_EQ(expected_roles, node1.get_roles());
  auto* other_addresses = node1.get_other_addresses();
  ASSERT_TRUE(other_addresses);
  ASSERT_EQ(SocketAddressFamily::UNIX,
            other_addresses->get_gossip()->get_address_family());
  ASSERT_EQ(SocketAddressFamily::UNIX,
            other_addresses->get_ssl()->get_address_family());
  ASSERT_TRUE(node1.get_storage());
  ASSERT_EQ(2, node1.get_storage()->get_num_shards());
  ASSERT_EQ(1.0, node1.get_storage()->get_weight());
  ASSERT_TRUE(node1.get_sequencer());
  ASSERT_EQ(1.0, node1.get_sequencer()->get_weight());

  // Finding by Location
  NodesConfigResponse response3;
  NodesFilter location_filter;
  location_filter.set_location("rg1.dc1.cl1.rw1.rk1");

  admin_client->sync_getNodesConfig(response3, location_filter);
  ASSERT_EQ(2, response3.get_nodes().size());

  location_filter.set_location("rg1.dc1.cl1");

  admin_client->sync_getNodesConfig(response3, location_filter);
  ASSERT_EQ(3, response3.get_nodes().size());

  location_filter.set_location("rg1.dc1.cl1.rw1.rk2");

  admin_client->sync_getNodesConfig(response3, location_filter);
  ASSERT_EQ(1, response3.get_nodes().size());

  // Finding by address
  NodesConfigResponse response4;
  NodesFilter address_filter;

  thrift::NodeID node_identifier2;
  node_identifier2.set_address(data_address);
  address_filter.set_node(node_identifier2);
  admin_client->sync_getNodesConfig(response4, address_filter);
  ASSERT_EQ(1, response4.get_nodes().size());
  ASSERT_EQ(0, response4.get_nodes()[0].get_node_index());

  // Multiple Filters
  NodesConfigResponse response5;
  NodesFilter mixed_filter;
  // matches 2 nodes
  mixed_filter.set_location("rg1.dc1.cl1.rw1.rk1");
  // matches node index 0
  thrift::NodeID node_identifier3;
  node_identifier3.set_address(data_address);
  mixed_filter.set_node(node_identifier3);

  admin_client->sync_getNodesConfig(response5, mixed_filter);
  ASSERT_EQ(1, response5.get_nodes().size());
  ASSERT_EQ(0, response5.get_nodes()[0].get_node_index());
}
