/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/ClusterMembershipAPIHandler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/admin/if/gen-cpp2/cluster_membership_constants.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/membership/gen-cpp2/Membership_types.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using namespace apache::thrift;
using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

class ClusterMemebershipAPIIntegrationTest : public IntegrationTestBase {
 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();

    cluster_ =
        IntegrationTestUtils::ClusterFactory()
            .useHashBasedSequencerAssignment()
            .enableSelfInitiatedRebuilding("1s")
            .setNodesConfigurationSourceOfTruth(
                IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
            .setParam("--event-log-grace-period", "1ms")
            .setParam("--disable-event-log-trimming", "true")
            .setParam("--enable-nodes-configuration-manager",
                      "true",
                      IntegrationTestUtils::ParamScope::ALL)
            .setParam("--nodes-configuration-manager-store-polling-interval",
                      "1s",
                      IntegrationTestUtils::ParamScope::ALL)
            .setParam("--nodes-configuration-manager-intermediary-shard-state-"
                      "timeout",
                      "2s")
            .runMaintenanceManagerOn(node_index_t(0))
            .setMetaDataLogsConfig(createMetaDataLogsConfig({2, 3}, 2))
            .deferStart()
            .create(4);
  }

  thrift::RemoveNodesRequest
  buildRemoveNodesRequest(std::vector<int32_t> idxs) {
    thrift::RemoveNodesRequest req;
    for (auto idx : idxs) {
      thrift::NodesFilter filter;
      filter.set_node(mkNodeID(node_index_t(idx)));
      req.node_filters.push_back(std::move(filter));
    }
    return req;
  }

  thrift::AddNodesRequest buildAddNodesRequest(std::vector<int32_t> idxs) {
    ld_assert(idxs.size() < 50);

    auto make_address = [](int addr, int port) {
      thrift::SocketAddress ret;
      ret.set_address(folly::sformat("127.0.0.{}", addr));
      ret.set_port(port);
      return ret;
    };

    thrift::AddNodesRequest req;
    for (int32_t idx : idxs) {
      thrift::NodeConfig cfg;

      cfg.set_node_index(idx);
      cfg.set_name(folly::sformat("server-{}", idx));
      cfg.set_data_address(make_address(0 + idx, 1000 + idx));

      {
        thrift::Addresses other_addresses;
        other_addresses.set_gossip(make_address(50 + idx, 2000 + idx));
        other_addresses.set_ssl(make_address(100 + idx, 3000 + idx));
        other_addresses.set_admin(make_address(150 + idx, 4000 + idx));
        cfg.set_other_addresses(other_addresses);
      }

      cfg.set_roles({thrift::Role::SEQUENCER, thrift::Role::STORAGE});

      {
        thrift::SequencerConfig seq_cfg;
        seq_cfg.set_weight(idx);
        cfg.set_sequencer(seq_cfg);
      }

      {
        thrift::StorageConfig storage_cfg;
        storage_cfg.set_weight(idx);
        storage_cfg.set_num_shards(2);
        cfg.set_storage(storage_cfg);
      }

      cfg.set_location(folly::sformat("PRN.PRN.PRN.PRN.{}", idx));

      thrift::AddSingleNodeRequest single;
      single.set_new_config(std::move(cfg));
      req.new_node_requests.push_back(std::move(single));
    }
    return req;
  }

  bool disableAndWait(std::vector<thrift::ShardID> shards,
                      std::vector<thrift::NodeID> sequencers) {
    auto admin_client = cluster_->getNode(0).createAdminClient();

    return wait_until("Maintenance manager disables the node", [&]() {
             thrift::MaintenanceDefinition request;
             request.set_user("bunny");
             request.set_shard_target_state(
                 thrift::ShardOperationalState::DRAINED);
             request.set_sequencer_nodes(sequencers);
             request.set_sequencer_target_state(
                 thrift::SequencingState::DISABLED);
             request.set_shards(shards);
             request.set_skip_safety_checks(true);
             thrift::MaintenanceDefinitionResponse resp;
             admin_client->sync_applyMaintenance(resp, request);
             return std::all_of(resp.get_maintenances().begin(),
                                resp.get_maintenances().end(),
                                [](const auto& m) {
                                  return m.progress ==
                                      thrift::MaintenanceProgress::COMPLETED;
                                });
           }) == 0;
  }

 public:
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
};

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveAliveNodes) {
  cluster_->updateNodeAttributes(
      node_index_t(1), configuration::StorageState::DISABLED, 1, false);
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();
  cluster_->getNode(0).waitUntilNodeStateReady();
  disableAndWait({mkShardID(1, -1)}, {mkNodeID(1)});

  try {
    thrift::RemoveNodesResponse resp;
    admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
    FAIL() << "RemoveNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(1, failed_node.node_id.node_index_ref().value_unchecked());
    EXPECT_EQ(
        thrift::ClusterMembershipFailureReason::NOT_DEAD, failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveNonExistentNode) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::RemoveNodesResponse resp;
  admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({10}));
  EXPECT_EQ(0, resp.removed_nodes.size());
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveEnabledNodes) {
  ASSERT_EQ(0, cluster_->start({0, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  try {
    thrift::RemoveNodesResponse resp;
    admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
    FAIL() << "RemoveNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(1, failed_node.node_id.node_index_ref().value_unchecked());
    EXPECT_EQ(thrift::ClusterMembershipFailureReason::NOT_DISABLED,
              failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveNodeSuccess) {
  cluster_->updateNodeAttributes(
      node_index_t(1), configuration::StorageState::DISABLED, 1, false);
  ASSERT_EQ(0, cluster_->start({0, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();
  cluster_->getNode(0).waitUntilNodeStateReady();
  disableAndWait({mkShardID(1, -1)}, {mkNodeID(1)});

  thrift::RemoveNodesResponse resp;
  admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
  EXPECT_EQ(1, resp.removed_nodes.size());
  EXPECT_EQ(1, resp.removed_nodes[0].node_index_ref().value_unchecked());

  wait_until("AdminServer's NC picks the removal", [&]() {
    thrift::NodesConfigResponse nodes_config;
    admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
    return nodes_config.version >= resp.new_nodes_configuration_version;
  });

  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
  EXPECT_EQ(3, nodes_config.nodes.size());
}

MATCHER_P2(NodeConfigEq, expected_idx, req, "") {
  return expected_idx == arg.node_index && req.name == arg.name &&
      req.data_address == arg.data_address &&
      req.other_addresses_ref() == arg.other_addresses_ref() &&
      req.location_ref() == arg.location_ref() && req.roles == arg.roles &&
      req.sequencer_ref() == arg.sequencer_ref() &&
      req.storage_ref() == arg.storage_ref();
};

TEST_F(ClusterMemebershipAPIIntegrationTest, TestAddNodeSuccess) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::AddNodesRequest req = buildAddNodesRequest({10, 50});
  // Let the admin server allocate the NodeID for the second node for us
  req.new_node_requests[1].new_config.set_node_index(
      thrift::cluster_membership_constants::ANY_NODE_IDX());
  thrift::AddNodesResponse resp;

  admin_client->sync_addNodes(resp, req);
  EXPECT_EQ(2, resp.added_nodes.size());
  EXPECT_THAT(resp.added_nodes,
              UnorderedElementsAre(
                  NodeConfigEq(10, req.new_node_requests[0].new_config),
                  NodeConfigEq(4, req.new_node_requests[1].new_config)));

  wait_until("AdminServer's NC picks the additions", [&]() {
    thrift::NodesConfigResponse nodes_config;
    admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
    return nodes_config.version >= resp.new_nodes_configuration_version;
  });

  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
  EXPECT_EQ(6, nodes_config.nodes.size());
  EXPECT_THAT(
      nodes_config.nodes,
      AllOf(Contains(NodeConfigEq(10, req.new_node_requests[0].new_config)),
            Contains(NodeConfigEq(4, req.new_node_requests[1].new_config))));
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestAddAlreadyExists) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  // Get current Admin server version
  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});

  thrift::AddNodesRequest req = buildAddNodesRequest({100});
  // Copy the address of an existing node
  req.new_node_requests[0].new_config.data_address =
      nodes_config.nodes[0].data_address;

  try {
    thrift::AddNodesResponse resp;
    admin_client->sync_addNodes(resp, req);
    FAIL() << "AddNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(100, failed_node.node_id.node_index_ref().value_unchecked());
    EXPECT_EQ(thrift::ClusterMembershipFailureReason::ALREADY_EXISTS,
              failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestInvalidAddNodesRequest) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  // Get current Admin server version
  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});

  thrift::AddNodesRequest req = buildAddNodesRequest({4});
  // Let's reset the storage the storage config
  req.new_node_requests[0].new_config.storage_ref().reset();

  try {
    thrift::AddNodesResponse resp;
    admin_client->sync_addNodes(resp, req);
    FAIL() << "AddNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(4, failed_node.node_id.node_index_ref().value_unchecked());
    EXPECT_EQ(
        thrift::ClusterMembershipFailureReason::INVALID_REQUEST_NODES_CONFIG,
        failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestUpdateRequest) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::NodesFilter filter;
  filter.set_node(mkNodeID(node_index_t(3)));
  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, filter);
  ASSERT_EQ(1, nodes_config.nodes.size());

  // Update N3
  auto cfg = nodes_config.nodes[0];
  cfg.set_name("updatedName");
  cfg.data_address.set_address("/test1");
  cfg.other_addresses_ref()->gossip_ref()->set_address("/test2");
  cfg.other_addresses_ref()->ssl_ref()->set_address("/test3");
  cfg.storage_ref()->set_weight(123);
  cfg.sequencer_ref()->set_weight(122);

  thrift::UpdateSingleNodeRequest updt;
  updt.set_node_to_be_updated(mkNodeID(3));
  updt.set_new_config(cfg);
  thrift::UpdateNodesRequest req;
  req.set_node_requests({std::move(updt)});

  thrift::UpdateNodesResponse uresp;
  admin_client->sync_updateNodes(uresp, req);
  EXPECT_EQ(1, uresp.updated_nodes.size());
  EXPECT_THAT(uresp.updated_nodes, UnorderedElementsAre(NodeConfigEq(3, cfg)));

  wait_until("AdminServer's NC picks the updates", [&]() {
    thrift::NodesConfigResponse nc;
    admin_client->sync_getNodesConfig(nc, thrift::NodesFilter{});
    return nc.version >= uresp.new_nodes_configuration_version;
  });

  admin_client->sync_getNodesConfig(nodes_config, filter);
  ASSERT_EQ(1, nodes_config.nodes.size());
  ASSERT_THAT(nodes_config.nodes[0], NodeConfigEq(3, cfg));
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestUpdateFailure) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::NodesFilter filter;
  filter.set_node(mkNodeID(node_index_t(3)));
  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, filter);
  ASSERT_EQ(1, nodes_config.nodes.size());

  auto cfg = nodes_config.nodes[0];
  thrift::UpdateSingleNodeRequest updt;
  updt.set_node_to_be_updated(mkNodeID(3));
  updt.set_new_config(cfg);
  thrift::UpdateNodesRequest _req;
  _req.set_node_requests({std::move(updt)});

  // The constant base for all the updates. Copy it and modify the request.
  const thrift::UpdateNodesRequest request_tpl{std::move(_req)};

  {
    // A mismatch in the node's index should fail.
    auto req = request_tpl;
    req.node_requests[0].set_node_to_be_updated(mkNodeID(2));

    try {
      thrift::UpdateNodesResponse resp;
      admin_client->sync_updateNodes(resp, req);
      FAIL() << "UpdateNodes call should fail, but it didn't";
    } catch (const thrift::ClusterMembershipOperationFailed& exception) {
      ASSERT_EQ(1, exception.failed_nodes.size());
      auto failed_node = exception.failed_nodes[0];
      EXPECT_EQ(2, failed_node.node_id.node_index_ref().value_unchecked());
      EXPECT_EQ(
          thrift::ClusterMembershipFailureReason::INVALID_REQUEST_NODES_CONFIG,
          failed_node.reason);
    }
  }

  {
    // Trying to update a node that doesn't exist should fail
    auto req = request_tpl;
    req.node_requests[0].set_node_to_be_updated(mkNodeID(20));

    try {
      thrift::UpdateNodesResponse resp;
      admin_client->sync_updateNodes(resp, req);
      FAIL() << "UpdateNodes call should fail, but it didn't";
    } catch (const thrift::ClusterMembershipOperationFailed& exception) {
      ASSERT_EQ(1, exception.failed_nodes.size());
      auto failed_node = exception.failed_nodes[0];
      EXPECT_EQ(20, failed_node.node_id.node_index_ref().value_unchecked());
      EXPECT_EQ(thrift::ClusterMembershipFailureReason::NO_MATCH_IN_CONFIG,
                failed_node.reason);
    }
  }

  {
    // Trying to update an immutable attribute (e.g location) will fail with an
    // NCM error.
    auto req = request_tpl;
    req.node_requests[0].new_config.set_location("FRC.FRC.FRC.FRC.FRC");

    try {
      thrift::UpdateNodesResponse resp;
      admin_client->sync_updateNodes(resp, req);
      FAIL() << "UpdateNodes call should fail, but it didn't";
    } catch (const thrift::NodesConfigurationManagerError& exception) {
      EXPECT_EQ(static_cast<int32_t>(E::INVALID_PARAM),
                exception.error_code_ref().value_unchecked());
    }
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, MarkShardsAsProvisionedSuccess) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  cluster_->getNode(0).waitUntilNodeStateReady();
  auto admin_client = cluster_->getNode(0).createAdminClient();

  {
    // Add two nodes with 2 shards each. They will get added as PROVISIONING.
    thrift::AddNodesResponse resp;
    admin_client->sync_addNodes(resp, buildAddNodesRequest({100, 101}));
    ASSERT_EQ(2, resp.added_nodes.size());

    wait_until("AdminServer's NC picks the additions", [&]() {
      thrift::NodesConfigResponse nc;
      admin_client->sync_getNodesConfig(nc, thrift::NodesFilter{});
      return nc.version >= resp.new_nodes_configuration_version;
    });
  }

  // Mark all N100 shards, and only N101:S0 as provisioned
  thrift::MarkShardsAsProvisionedRequest req;
  req.set_shards({mkShardID(100, -1), mkShardID(101, 0)});

  thrift::MarkShardsAsProvisionedResponse resp;

  /* Retry as long as it's a VERSION_MISMATCH */
  bool failed = false;
  for (size_t trials = 0; trials < 10; trials++) {
    failed = false;
    try {
      admin_client->sync_markShardsAsProvisioned(resp, req);
      break;
    } catch (const thrift::NodesConfigurationManagerError& ex) {
      ASSERT_EQ(static_cast<int32_t>(E::VERSION_MISMATCH),
                ex.error_code_ref().value_unchecked());
      failed = true;
    }
  }

  ASSERT_FALSE(failed);
  EXPECT_THAT(resp.get_updated_shards(),
              UnorderedElementsAre(
                  mkShardID(100, 0), mkShardID(100, 1), mkShardID(101, 0)));

  wait_until("AdminServer's NC picks the updates", [&]() {
    thrift::NodesConfigResponse nc;
    admin_client->sync_getNodesConfig(nc, thrift::NodesFilter{});
    return nc.version >= resp.new_nodes_configuration_version;
  });

  auto get_shard_state = [&](const thrift::NodesState& state,
                             thrift::ShardID shard) {
    for (const auto& node : state) {
      if (node.get_config().get_node_index() ==
          *shard.get_node().node_index_ref()) {
        return node.shard_states_ref()
            ->at(shard.get_shard_index())
            .get_storage_state();
      }
    }
    return membership::thrift::StorageState::INVALID;
  };

  thrift::NodesStateRequest state_req;
  state_req.set_filter(thrift::NodesFilter{});

  thrift::NodesStateResponse nc;
  admin_client->sync_getNodesState(nc, state_req);

  // MM could have already started enabling the shards after getting out of the
  // PROVISIONING state. So it's just enough to check that they are not in
  // PROVISIONING anymore.
  EXPECT_NE(membership::thrift::StorageState::PROVISIONING,
            get_shard_state(nc.get_states(), mkShardID(100, 0)));
  EXPECT_NE(membership::thrift::StorageState::PROVISIONING,
            get_shard_state(nc.get_states(), mkShardID(100, 1)));
  EXPECT_NE(membership::thrift::StorageState::PROVISIONING,
            get_shard_state(nc.get_states(), mkShardID(101, 0)));
  EXPECT_EQ(membership::thrift::StorageState::PROVISIONING,
            get_shard_state(nc.get_states(), mkShardID(101, 1)));
}
