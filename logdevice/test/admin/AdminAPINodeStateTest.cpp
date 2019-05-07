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

class AdminAPINodeStateTest : public IntegrationTestBase {};

const int NUM_DB_SHARDS = 3;

TEST_F(AdminAPINodeStateTest, getNodeState) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setNumDBShards(NUM_DB_SHARDS)
                     .setParam("--disable-rebuilding", "false")
                     .setParam("--gossip-interval", "5ms")
                     .create(3);

  for (const auto& it : cluster->getNodes()) {
    node_index_t idx = it.first;
    cluster->getNode(idx).waitUntilAvailable();
  }

  cluster->waitForRecovery();
  folly::EventBase eventBase;
  auto admin_client = create_admin_client(&eventBase, cluster.get(), 1);
  ASSERT_NE(nullptr, admin_client);

  wait_until(
      "LogDevice started but we are waiting for the EventLog to be replayed",
      [&]() {
        try {
          NodesStateRequest req;
          NodesStateResponse resp;
          admin_client->sync_getNodesState(resp, req);
          return true;
        } catch (thrift::NodeNotReady& e) {
          return false;
        }
      });

  NodesStateRequest request;
  NodesStateResponse response;
  admin_client->sync_getNodesState(response, request);
  ASSERT_EQ(3, response.get_states().size());

  NodesFilter filter;
  thrift::NodeID node_identifier;
  node_identifier.set_node_index(0);
  filter.set_node(node_identifier);
  request.set_filter(filter);
  admin_client->sync_getNodesState(response, request);
  ASSERT_EQ(1, response.get_states().size());
  auto node1 = response.get_states()[0];
  ASSERT_EQ(0, node1.get_node_index());
  ASSERT_TRUE(node1.get_daemon_state());
  ASSERT_EQ(ServiceState::ALIVE, *node1.get_daemon_state());
  ASSERT_TRUE(node1.get_sequencer_state());
  const SequencerState* seq_state = node1.get_sequencer_state();
  ASSERT_EQ(SequencingState::ENABLED, seq_state->get_state());
  ASSERT_TRUE(node1.get_shard_states());
  const auto* shard_states = node1.get_shard_states();
  ASSERT_EQ(3, shard_states->size());
  for (auto shard : *shard_states) {
    ASSERT_EQ(ShardDataHealth::HEALTHY, shard.get_data_health());
    ASSERT_EQ(membership::thrift::StorageState::READ_WRITE,
              shard.get_storage_state());
    ASSERT_EQ(membership::thrift::MetaDataStorageState::METADATA,
              shard.get_metadata_state());
    ASSERT_EQ(
        ShardOperationalState::ENABLED, shard.get_current_operational_state());
  }

  // let's kill one node
  cluster->shutdownNodes({0});
  wait_until("ServiceState should go down to DEAD when node 0 is killed",
             [&]() -> bool {
               NodesStateResponse resp;
               NodesStateRequest req;
               NodesFilter fltr;
               thrift::NodeID ident;
               ident.set_node_index(0);
               fltr.set_node(ident);
               req.set_filter(fltr);
               admin_client->sync_getNodesState(resp, req);
               auto node = resp.get_states()[0];
               return (ServiceState::DEAD == *node.get_daemon_state());
             });
}
