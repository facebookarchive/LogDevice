/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/Conv.h"
#include "logdevice/admin/if/gen-cpp2/maintenance_types_custom_protocol.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#include "thrift/lib/cpp2/protocol/DebugProtocol.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

#define LOG_ID logid_t(1)

constexpr node_index_t maintenance_leader{2};

class MaintenanceAPITestDisabled : public IntegrationTestBase {};

TEST_F(MaintenanceAPITestDisabled, NotSupported) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setNumDBShards(1)
                     .setParam("--enable-maintenance-manager", "false")
                     .setParam("--gossip-interval", "5ms")
                     .create(1);

  cluster->waitUntilAllAvailable();
  auto& node = cluster->getNode(0);
  auto admin_client = node.createAdminClient();

  thrift::MaintenanceDefinitionResponse response;
  thrift::MaintenancesFilter filter;
  ASSERT_THROW(admin_client->sync_getMaintenances(response, filter),
               thrift::NotSupported);
}

class MaintenanceAPITest : public IntegrationTestBase {
 public:
  void init();
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;

 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();
  }
};

void MaintenanceAPITest::init() {
  const size_t num_nodes = 6;
  const size_t num_shards = 2;

  Configuration::Nodes nodes;

  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(num_shards);
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);

  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_replicationFactor(3);
  internal_log_attrs.set_extraCopies(0);
  internal_log_attrs.set_syncedCopies(0);
  internal_log_attrs.set_maxWritesInFlight(2048);

  auto meta_configs =
      createMetaDataLogsConfig({0, 1, 2, 3, 4, 5}, 3, NodeLocationScope::NODE);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(100)
          .setNodes(nodes)
          .setNodesConfigurationSourceOfTruth(
              IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
          .enableSelfInitiatedRebuilding("3600s")
          .setParam("--max-node-rebuilding-percentage", "50")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--enable-safety-check-periodic-metadata-update", "true")
          .setParam("--disable-event-log-trimming", "true")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "0")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--max-unavailable-storage-capacity-pct", "80")
          .setParam("--loglevel", "debug")
          // Starts MaintenanceManager on N2
          .runMaintenanceManagerOn(maintenance_leader)
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
          .setMaintenanceLogAttributes(internal_log_attrs)
          .setEventLogAttributes(internal_log_attrs)
          .setConfigLogAttributes(internal_log_attrs)
          .setMetaDataLogsConfig(meta_configs)
          .deferStart()
          .create(num_nodes);
}

TEST_F(MaintenanceAPITest, ApplyMaintenancesInvalid1) {
  init();
  cluster_->start();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Invalid maintenance
  {
    thrift::MaintenanceDefinition def;
    thrift::MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        admin_client->sync_applyMaintenance(resp, def), thrift::InvalidRequest);
  }
  {
    thrift::MaintenanceDefinition def;
    thrift::ShardSet shards;
    shards.push_back(mkShardID(1, -1));
    def.set_shards(shards);
    def.set_shard_target_state(ShardOperationalState::DRAINED);

    thrift::MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since user is not set for this maintenance
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
  }
  // Apply maintenance for a sequencer NodeID that is unset.
  {
    thrift::MaintenanceDefinition def;
    def.set_user("my-user");
    def.set_sequencer_nodes({thrift::NodeID()});
    def.set_sequencer_target_state(SequencingState::DISABLED);

    thrift::MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since sequencers have empty NodeID
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
  }
  // Apply maintenance for a ShardID that has only the shard part set.
  {
    thrift::MaintenanceDefinition def;
    def.set_user("my-user");
    thrift::NodeID node; // has nothing set.
    thrift::ShardID shard;
    shard.set_shard_index(-1);
    shard.set_node(node);
    def.set_shards({shard});
    def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);

    thrift::MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since sequencers have empty NodeID
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
  }
}

TEST_F(MaintenanceAPITest, ApplyMaintenancesValidNoClash) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();
  std::string created_id;
  int64_t created_on;
  {
    thrift::MaintenanceDefinition request;
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(1, -1)});
    request.set_sequencer_nodes({mkNodeID(1)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_skip_safety_checks(true);
    request.set_group(false);
    // We expect this to be expanded into 1 maintenance group since everything
    // fits nicely into a single node.
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const thrift::MaintenanceDefinition& result = output[0];
    ASSERT_EQ("bunny", result.get_user());
    ASSERT_TRUE(result.group_id_ref().has_value());
    created_id = result.group_id_ref().value();
    ld_info("Maintenance created: %s", result.group_id_ref().value().c_str());
    ASSERT_EQ(8, result.group_id_ref().value().size());
    // We have 2 shards per node.
    ASSERT_THAT(result.get_shards(),
                UnorderedElementsAre(mkShardID(1, 0), mkShardID(1, 1)));
    ASSERT_EQ(ShardOperationalState::DRAINED, result.get_shard_target_state());
    ASSERT_EQ(SequencingState::DISABLED, result.get_sequencer_target_state());
    ASSERT_THAT(
        result.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
    ASSERT_TRUE(result.get_skip_safety_checks());
    ASSERT_TRUE(result.created_on_ref().has_value());
    ASSERT_TRUE(result.created_on_ref().value() > 0);
    created_on = result.created_on_ref().value();
  }
  {
    // Verify that applying the same maintenance does not result in clash
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, thrift::MaintenancesFilter());
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size()) << apache::thrift::debugString(resp);
    const MaintenanceDefinition& result = output[0];
    thrift::MaintenanceDefinition request;
    request.set_user(result.get_user());
    request.set_shard_target_state(result.get_shard_target_state());
    // expands to all shards of node 1
    request.set_shards(result.get_shards());
    request.set_sequencer_nodes(result.get_sequencer_nodes());
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_skip_safety_checks(true);
    request.set_group(false);
    // We expect this to be expanded into 1 maintenance group since everything
    // fits nicely into a single node.
    thrift::MaintenanceDefinitionResponse resp2;
    admin_client->sync_applyMaintenance(resp2, request);
    output = resp2.get_maintenances();
    ASSERT_EQ(1, output.size());
    const thrift::MaintenanceDefinition& result2 = output[0];
    ASSERT_EQ("bunny", result2.get_user());
    ASSERT_EQ(created_id, result2.group_id_ref().value().c_str());
  }
}

TEST_F(MaintenanceAPITest, ApplyMaintenancesValid) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();
  std::string created_id;
  int64_t created_on;
  {
    thrift::MaintenanceDefinition request;
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(1, -1)});
    request.set_sequencer_nodes({mkNodeID(1)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_skip_safety_checks(true);
    request.set_group(false);
    // We expect this to be expanded into 1 maintenance group since everything
    // fits nicely into a single node.
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const thrift::MaintenanceDefinition& result = output[0];
    ASSERT_EQ("bunny", result.get_user());
    ASSERT_TRUE(result.group_id_ref().has_value());
    created_id = result.group_id_ref().value();
    ld_info("Maintenance created: %s", result.group_id_ref().value().c_str());
    ASSERT_EQ(8, result.group_id_ref().value().size());
    // We have 2 shards per node.
    ASSERT_THAT(result.get_shards(),
                UnorderedElementsAre(mkShardID(1, 0), mkShardID(1, 1)));
    ASSERT_EQ(ShardOperationalState::DRAINED, result.get_shard_target_state());
    ASSERT_EQ(SequencingState::DISABLED, result.get_sequencer_target_state());
    ASSERT_THAT(
        result.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
    ASSERT_TRUE(result.get_skip_safety_checks());
    ASSERT_TRUE(result.created_on_ref().has_value());
    ASSERT_TRUE(result.created_on_ref().value() > 0);
    created_on = result.created_on_ref().value();
  }
  {
    // Validate via getMaintenances
    thrift::MaintenancesFilter fltr;
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, fltr);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const thrift::MaintenanceDefinition& result = output[0];
    ASSERT_EQ("bunny", result.get_user());
    ASSERT_TRUE(result.group_id_ref().has_value());
    ASSERT_EQ(created_id, result.group_id_ref().value());
    // We have 2 shards per node.
    ASSERT_THAT(result.get_shards(),
                UnorderedElementsAre(mkShardID(1, 0), mkShardID(1, 1)));
    ASSERT_EQ(ShardOperationalState::DRAINED, result.get_shard_target_state());
    ASSERT_EQ(SequencingState::DISABLED, result.get_sequencer_target_state());
    ASSERT_THAT(
        result.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
    ASSERT_TRUE(result.get_skip_safety_checks());
    ASSERT_TRUE(result.created_on_ref().has_value());
    ASSERT_TRUE(result.created_on_ref().value() > 0);
    wait_until("Maintenance transition to COMPLETED", [&]() {
      thrift::MaintenancesFilter req1;
      thrift::MaintenanceDefinitionResponse resp1;
      admin_client->sync_getMaintenances(resp1, req1);
      const auto& def = resp1.get_maintenances()[0];
      return def.get_progress() == thrift::MaintenanceProgress::COMPLETED;
    });
  }
  // Let's create another maintenance at which one will match with the existing.
  std::string created_id2;
  {
    thrift::MaintenanceDefinition request;
    // Needs to be the same user for us to match an existing maintenance.
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1 (existing), and node 2 (new)
    request.set_shards({mkShardID(1, -1), mkShardID(2, -1)});
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(2)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    // if group=True, this maintenance will CLASH. We will test that after this.
    request.set_group(false);
    // We expect to get two maintenances back:
    //  - An existing maintenance with the group_id (created_id) for node (1)
    //  - A new maintenance for node (2)
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(2, output.size());
    bool found_existing = false;
    for (const auto& def : output) {
      ASSERT_EQ("bunny", def.get_user());
      ASSERT_TRUE(def.group_id_ref().has_value());
      ASSERT_EQ(ShardOperationalState::DRAINED, def.get_shard_target_state());
      ASSERT_EQ(SequencingState::DISABLED, def.get_sequencer_target_state());
      if (def.group_id_ref().value() == created_id) {
        found_existing = true;
        ASSERT_EQ(created_on, def.created_on_ref().value());
        // We have 2 shards per node.
        ASSERT_THAT(def.get_shards(),
                    UnorderedElementsAre(mkShardID(1, 0), mkShardID(1, 1)));
        def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1));
        // the original request skipped safety checks.
        ASSERT_TRUE(def.get_skip_safety_checks());
      } else {
        ASSERT_EQ(8, def.group_id_ref().value().size());
        created_id2 = def.group_id_ref().value();
        // We have 2 shards per node.
        ASSERT_THAT(def.get_shards(),
                    UnorderedElementsAre(mkShardID(2, 0), mkShardID(2, 1)));
        def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(2));
        ASSERT_FALSE(def.get_skip_safety_checks());
      }
    }
    ASSERT_TRUE(found_existing);
  }
  {
    // Validate via getMaintenances
    thrift::MaintenancesFilter fltr;
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, fltr);
    auto output = resp.get_maintenances();
    ASSERT_EQ(2, output.size());
    std::unordered_set<std::string> returned_ids;
    for (const auto& def : output) {
      returned_ids.insert(def.group_id_ref().value());
    }
    ASSERT_THAT(returned_ids, UnorderedElementsAre(created_id, created_id2));
  }
  {
    // CLASH -- let's create a maintenance with the same user that overlap with
    // existing one but not identical.
    //
    thrift::MaintenanceDefinition request;
    // Needs to be the same user for us to match an existing maintenance.
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1 (existing / clash), and node 3 (new)
    request.set_shards({mkShardID(1, -1), mkShardID(3, -1)});
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(2)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // This will make this maintenance clash with existing ones since there is
    // no single group that match such maintenance.
    request.set_group(true);
    thrift::MaintenanceDefinitionResponse resp;
    ASSERT_THROW(admin_client->sync_applyMaintenance(resp, request),
                 thrift::MaintenanceClash);
  }
}

TEST_F(MaintenanceAPITest, ApplyMaintenancesSafetyCheckResults) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();

  // Creating an impossible maintenance, draining all shards in all nodes must
  // fail by safety checker.
  thrift::MaintenanceDefinition request;
  // Needs to be the same user for us to match an existing maintenance.
  request.set_user("dummy");
  request.set_shard_target_state(ShardOperationalState::DRAINED);
  request.set_shards({mkShardID(0, -1),
                      mkShardID(1, -1),
                      mkShardID(2, -1),
                      mkShardID(3, -1),
                      mkShardID(4, -1)});
  request.set_sequencer_nodes({mkNodeID(1), mkNodeID(2)});
  request.set_sequencer_target_state(SequencingState::DISABLED);
  request.set_group(true);
  thrift::MaintenanceDefinitionResponse resp;
  admin_client->sync_applyMaintenance(resp, request);
  ASSERT_EQ(1, resp.get_maintenances().size());
  wait_until("MaintenanceManager runs the workflow", [&]() {
    thrift::MaintenancesFilter req1;
    thrift::MaintenanceDefinitionResponse resp1;
    admin_client->sync_getMaintenances(resp1, req1);
    const auto& def = resp1.get_maintenances()[0];
    return thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE ==
        def.get_progress();
  });
  thrift::MaintenancesFilter req;
  admin_client->sync_getMaintenances(resp, req);
  const auto& def = resp.get_maintenances()[0];
  ASSERT_TRUE(def.last_check_impact_result_ref().has_value());
  const auto& check_impact_result = def.last_check_impact_result_ref().value();
  ASSERT_TRUE(check_impact_result.get_impact().size() > 0);
  ASSERT_EQ(
      thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE, def.get_progress());
}

TEST_F(MaintenanceAPITest, MayDisappearInSequencerFailsSafetyCheck) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();

  // Create a maintenance that sets N1 to MAY_DISAPPEAR
  thrift::MaintenanceDefinition request1;
  request1.set_user("user1");
  request1.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  request1.set_shards({mkShardID(1, -1)});
  request1.set_sequencer_nodes({mkNodeID(1)});
  request1.set_sequencer_target_state(SequencingState::DISABLED);
  request1.set_group(true);
  thrift::MaintenanceDefinitionResponse resp1;
  admin_client->sync_applyMaintenance(resp1, request1);
  ASSERT_EQ(1, resp1.get_maintenances().size());

  wait_until("Maintenance for N1 transitions to COMPLETED", [&]() {
    thrift::MaintenancesFilter request2;
    request2.set_user("user1");
    thrift::MaintenanceDefinitionResponse resp2;
    admin_client->sync_getMaintenances(resp2, request2);
    const auto& def = resp2.get_maintenances()[0];
    return thrift::MaintenanceProgress::COMPLETED == def.get_progress();
  });

  // Create a maintenance that sets N3 to MAY_DISAPPEAR
  // This should fail safety check becasue we will lose read
  // availability with 2 nodes in MAY_DISAPPEAR
  thrift::MaintenanceDefinition request3;
  request3.set_user("user2");
  request3.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  request3.set_shards({mkShardID(3, -1)});
  request3.set_sequencer_target_state(SequencingState::DISABLED);
  request3.set_group(true);
  thrift::MaintenanceDefinitionResponse resp3;
  admin_client->sync_applyMaintenance(resp3, request3);
  ASSERT_EQ(1, resp3.get_maintenances().size());

  wait_until("Maintenance for N3 transitions to BLOCKED_UNTIL_SAFE", [&]() {
    thrift::MaintenancesFilter request4;
    request4.set_user("user2");
    thrift::MaintenanceDefinitionResponse resp4;
    admin_client->sync_getMaintenances(resp4, request4);
    const auto& def = resp4.get_maintenances()[0];
    return thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE ==
        def.get_progress();
  });

  // Now create a maintenance to set N4 to MAY_DISAPPEAR.
  // This should fail safety check becasue we will lose read
  // availability with 3 nodes in MAY_DISAPPEAR
  thrift::MaintenanceDefinition request4;
  request4.set_user("user3");
  request4.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  request4.set_shards({mkShardID(4, -1)});
  request4.set_sequencer_target_state(SequencingState::DISABLED);
  request4.set_group(true);
  thrift::MaintenanceDefinitionResponse resp4;
  admin_client->sync_applyMaintenance(resp4, request4);
  ASSERT_EQ(1, resp4.get_maintenances().size());

  wait_until("Maintenance for N4 transitions to BLOCKED_UNTIL_SAFE", [&]() {
    thrift::MaintenancesFilter request5;
    request5.set_user("user3");
    thrift::MaintenanceDefinitionResponse resp5;
    admin_client->sync_getMaintenances(resp5, request5);
    const auto& def = resp5.get_maintenances()[0];
    return thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE ==
        def.get_progress();
  });

  // Remove Maintenance from user1
  thrift::RemoveMaintenancesRequest request6;
  thrift::MaintenancesFilter filter6;
  filter6.set_user("user1");
  request6.set_filter(filter6);
  thrift::RemoveMaintenancesResponse resp6;
  admin_client->sync_removeMaintenances(resp6, request6);
  // Let's wait until N1 is ENABLED again
  {
    thrift::NodesFilter filter;
    thrift::NodeID node_id;
    node_id.set_node_index(1);
    filter.set_node(node_id);
    thrift::NodesStateRequest request;
    request.set_filter(filter);
    thrift::NodesStateResponse response;
    wait_until("All Shards in N1 are ENABLED", [&]() {
      try {
        admin_client->sync_getNodesState(response, request);
        const auto& state = response.get_states()[0];
        const auto& shard_states = state.shard_states_ref().value();
        bool all_finished = true;
        for (const auto& shard : shard_states) {
          if (shard.get_storage_state() !=
              membership::thrift::StorageState::READ_WRITE) {
            all_finished = false;
          }
        }
        return all_finished == true;
      } catch (thrift::NodeNotReady& e) {
        return false;
      }
      return false;
    });
  }

  // Verify that maintenance for N4 is still blocked
  thrift::MaintenancesFilter request7;
  request7.set_user("user3");
  thrift::MaintenanceDefinitionResponse resp7;
  admin_client->sync_getMaintenances(resp7, request7);
  const auto& def = resp7.get_maintenances()[0];
  ASSERT_TRUE(def.last_check_impact_result_ref().has_value());
  const auto& check_impact_result = def.last_check_impact_result_ref().value();
  ASSERT_TRUE(check_impact_result.get_impact().size() > 0);
  ASSERT_EQ(
      thrift::MaintenanceProgress::BLOCKED_UNTIL_SAFE, def.get_progress());

  // Remove Maintenance from user2
  thrift::RemoveMaintenancesRequest request8;
  thrift::MaintenancesFilter filter8;
  filter8.set_user("user2");
  request8.set_filter(filter8);
  thrift::RemoveMaintenancesResponse resp8;
  admin_client->sync_removeMaintenances(resp8, request8);

  // Now N4's maintenance should be unblocked and run to completion
  wait_until("Maintenance for N4 transitions to COMPLETED", [&]() {
    thrift::MaintenancesFilter request9;
    request9.set_user("user3");
    thrift::MaintenanceDefinitionResponse resp9;
    admin_client->sync_getMaintenances(resp9, request9);
    const auto& tmpdef = resp9.get_maintenances()[0];
    return thrift::MaintenanceProgress::COMPLETED == tmpdef.get_progress();
  });
}

TEST_F(MaintenanceAPITest, RemoveMaintenancesInvalid) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();
  // We simply can't delete with an empty filter;
  thrift::RemoveMaintenancesRequest request;
  thrift::RemoveMaintenancesResponse resp;
  ASSERT_THROW(admin_client->sync_removeMaintenances(resp, request),
               thrift::InvalidRequest);
}

TEST_F(MaintenanceAPITest, RemoveMaintenances) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();
  std::string created_id;
  std::vector<std::string> bunny_group_ids;
  {
    thrift::MaintenanceDefinition request;
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(1, -1)});
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(2), mkNodeID(3)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_group(false);
    // We expect this to be expanded into 3 maintenance groups
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(3, output.size());
    for (const auto& def : output) {
      bunny_group_ids.push_back(def.group_id_ref().value());
    }
  }
  // Let's add another maintenance for another user. (in total we should have
  // 4 after this)
  std::string fourth_group;
  {
    thrift::MaintenanceDefinition request;
    request.set_user("dummy");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(3, -1)});
    request.set_sequencer_nodes({mkNodeID(1)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_group(true);
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    fourth_group = output[0].group_id_ref().value();
  }
  // Let's remove by a group_id. We will remove one of bunny's maintenances.
  {
    thrift::RemoveMaintenancesRequest request;
    thrift::MaintenancesFilter fltr;
    auto group_to_remove = bunny_group_ids.back();
    bunny_group_ids.pop_back();
    fltr.set_group_ids({group_to_remove});
    request.set_filter(fltr);

    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    ASSERT_EQ(group_to_remove, output[0].group_id_ref().value());
    // validate by getMaintenances.
    thrift::MaintenancesFilter req;
    thrift::MaintenanceDefinitionResponse response;
    admin_client->sync_getMaintenances(response, req);
    ASSERT_EQ(3, response.get_maintenances().size());
    for (const auto& def : response.get_maintenances()) {
      ASSERT_NE(group_to_remove, def.group_id_ref().value());
    }
  }
  // Let's remove all bunny's maintenances.
  {
    thrift::RemoveMaintenancesRequest request;
    thrift::MaintenancesFilter fltr;
    fltr.set_user("bunny");
    request.set_filter(fltr);
    thrift::RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, request);
    auto output = resp.get_maintenances();
    // two maintenance still belong to this user.
    ASSERT_EQ(2, output.size());
    for (const auto& def : output) {
      ASSERT_EQ("bunny", def.get_user());
    }
  }
  // Should we have one left, should belong to dummy
  {
    // validate by getMaintenances.
    thrift::MaintenancesFilter req;
    thrift::MaintenanceDefinitionResponse response;
    admin_client->sync_getMaintenances(response, req);
    ASSERT_EQ(1, response.get_maintenances().size());
    ASSERT_EQ(
        fourth_group, response.get_maintenances()[0].group_id_ref().value());
  }
}

TEST_F(MaintenanceAPITest, GetNodeState) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();

  wait_until("MaintenanceManager is ready", [&]() {
    thrift::NodesStateRequest req;
    thrift::NodesStateResponse resp;
    // Under stress runs, the initial initialization might take a while, let's
    // be patient and increase the timeout here.
    auto rpc_options = apache::thrift::RpcOptions();
    rpc_options.setTimeout(std::chrono::minutes(1));
    try {
      admin_client->sync_getNodesState(rpc_options, resp, req);
      if (resp.get_states().size() > 0) {
        const auto& state = resp.get_states()[0];
        if (thrift::ServiceState::ALIVE == state.get_daemon_state()) {
          return true;
        }
      }
      return false;
    } catch (thrift::NodeNotReady& e) {
      return false;
    }
  });

  {
    thrift::NodesStateRequest request;
    thrift::NodesStateResponse response;
    admin_client->sync_getNodesState(response, request);
    ASSERT_EQ(6, response.get_states().size());
    for (const auto& state : response.get_states()) {
      ASSERT_EQ(thrift::ServiceState::ALIVE, state.get_daemon_state());
      const thrift::SequencerState& seq_state =
          state.sequencer_state_ref().value();
      ASSERT_EQ(SequencingState::ENABLED, seq_state.get_state());
      const auto& shard_states = state.shard_states_ref().value();
      ASSERT_EQ(2, shard_states.size());
      for (const auto& shard : shard_states) {
        ASSERT_EQ(ShardDataHealth::HEALTHY, shard.get_data_health());
        ASSERT_EQ(thrift::ShardStorageState::READ_WRITE,
                  shard.get_current_storage_state());
        ASSERT_EQ(membership::thrift::StorageState::READ_WRITE,
                  shard.get_storage_state());
        ASSERT_EQ(membership::thrift::MetaDataStorageState::METADATA,
                  shard.get_metadata_state());
        ASSERT_EQ(ShardOperationalState::ENABLED,
                  shard.get_current_operational_state());
      }
    }
  }

  std::string group_id;
  {
    thrift::MaintenanceDefinition maintenance1;
    maintenance1.set_user("dummy");
    maintenance1.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    maintenance1.set_shards({mkShardID(1, -1)});
    maintenance1.set_sequencer_nodes({mkNodeID(1)});
    maintenance1.set_sequencer_target_state(SequencingState::DISABLED);
    thrift::MaintenanceDefinitionResponse created;
    admin_client->sync_applyMaintenance(created, maintenance1);
    ASSERT_EQ(1, created.get_maintenances().size());
    group_id = created.get_maintenances()[0].group_id_ref().value();
  }
  // Let's wait until the maintenance is applied.
  {
    thrift::NodesFilter filter;
    thrift::NodeID node_id;
    node_id.set_node_index(1);
    filter.set_node(node_id);
    thrift::NodesStateRequest request;
    request.set_filter(filter);
    thrift::NodesStateResponse response;
    wait_until("DRAIN of node 1 is complete", [&]() {
      try {
        admin_client->sync_getNodesState(response, request);
        const auto& state = response.get_states()[0];
        const auto& shard_states = state.shard_states_ref().value();
        // We need to wait for all shard maintenances to finish
        bool all_finished = true;
        for (const auto& shard : shard_states) {
          if (shard.maintenance_ref().has_value()) {
            const auto& maintenance_progress = shard.maintenance_ref().value();
            if (maintenance_progress.get_status() !=
                MaintenanceStatus::COMPLETED) {
              all_finished = false;
            }
          }
        }
        return all_finished == true;
      } catch (thrift::NodeNotReady& e) {
        return false;
      }
      return false;
    });
    ASSERT_EQ(1, response.get_states().size());
    const auto& state = response.get_states()[0];
    ASSERT_EQ(thrift::ServiceState::ALIVE, state.get_daemon_state());
    const auto& shard_states = state.shard_states_ref().value();
    ASSERT_EQ(2, shard_states.size());
    for (const auto& shard : shard_states) {
      ASSERT_EQ(
          membership::thrift::StorageState::NONE, shard.get_storage_state());
      // The deprecated ShardStorageState
      ASSERT_EQ(thrift::ShardStorageState::DISABLED,
                shard.get_current_storage_state());
      ASSERT_EQ(ShardDataHealth::EMPTY, shard.get_data_health());
      ASSERT_EQ(membership::thrift::MetaDataStorageState::METADATA,
                shard.get_metadata_state());
      ASSERT_EQ(ShardOperationalState::DRAINED,
                shard.get_current_operational_state());
      ASSERT_TRUE(shard.maintenance_ref().has_value());
      const auto& maintenance_progress = shard.maintenance_ref().value();
      ASSERT_EQ(
          MaintenanceStatus::COMPLETED, maintenance_progress.get_status());
      ASSERT_THAT(maintenance_progress.get_target_states(),
                  UnorderedElementsAre(ShardOperationalState::DRAINED));
      ASSERT_THAT(maintenance_progress.get_associated_group_ids(),
                  UnorderedElementsAre(group_id));
    }
    thrift::MaintenancesFilter fltr;
    thrift::MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, fltr);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const thrift::MaintenanceDefinition& result = output[0];
    ASSERT_EQ(thrift::MaintenanceProgress::COMPLETED, result.get_progress());
  }
}

// Enure a non authoritative rebuilding completes and a node is transitioned
TEST_F(MaintenanceAPITest, unblockRebuilding) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();

  wait_until("MaintenanceManager is ready", [&]() {
    thrift::NodesStateRequest req;
    thrift::NodesStateResponse resp;
    // Under stress runs, the initial initialization might take a while, let's
    // be patient and increase the timeout here.
    auto rpc_options = apache::thrift::RpcOptions();
    rpc_options.setTimeout(std::chrono::minutes(1));
    try {
      admin_client->sync_getNodesState(rpc_options, resp, req);
      if (resp.get_states().size() > 0) {
        const auto& state = resp.get_states()[0];
        if (thrift::ServiceState::ALIVE == state.get_daemon_state()) {
          return true;
        }
      }
      return false;
    } catch (thrift::NodeNotReady& e) {
      return false;
    }
  });

  {
    thrift::NodesStateRequest request;
    thrift::NodesStateResponse response;
    admin_client->sync_getNodesState(response, request);
    ASSERT_EQ(5, response.get_states().size());
    for (const auto& state : response.get_states()) {
      ASSERT_EQ(thrift::ServiceState::ALIVE, state.get_daemon_state());
      const thrift::SequencerState& seq_state =
          state.sequencer_state_ref().value();
      ASSERT_EQ(SequencingState::ENABLED, seq_state.get_state());
      const auto& shard_states = state.shard_states_ref().value();
      ASSERT_EQ(2, shard_states.size());
      for (const auto& shard : shard_states) {
        ASSERT_EQ(ShardDataHealth::HEALTHY, shard.get_data_health());
        ASSERT_EQ(thrift::ShardStorageState::READ_WRITE,
                  shard.get_current_storage_state());
        ASSERT_EQ(membership::thrift::StorageState::READ_WRITE,
                  shard.get_storage_state());
        ASSERT_EQ(membership::thrift::MetaDataStorageState::METADATA,
                  shard.get_metadata_state());
        ASSERT_EQ(ShardOperationalState::ENABLED,
                  shard.get_current_operational_state());
      }
    }
  }

  // Write some records
  ld_info("Creating client");
  auto client = cluster_->createClient();
  ld_info("Writing records");
  for (int i = 1; i <= 1000; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
  }

  // Let's apply a maintenance
  cluster_->getNode(0).kill();
  cluster_->getNode(1).kill();

  // Make self-initiated rebuilding quicker.
  for (const auto& kv : cluster_->getNodes()) {
    if (kv.first != 0 && kv.first != 1) {
      kv.second->updateSetting("self-initiated-rebuilding-grace-period", "5s");
    }
  }

  StorageSet expected_shards;
  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 2; j++) {
      expected_shards.push_back(ShardID(i, j));
    }
  }

  // Let's wait until the maintenance is applied.
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, expected_shards, AuthoritativeStatus::UNAVAILABLE, true);

  // wait till the MaintenanceStatus is REBUILDING_IS_BLOCKED
  {
    thrift::NodesFilter filter;
    thrift::NodeID node_id;
    node_id.set_node_index(0);
    filter.set_node(node_id);
    thrift::NodesStateRequest request;
    request.set_filter(filter);
    thrift::NodesStateResponse response;
    wait_until("Maintenance Status is REBUILDING_IS_BLOCKED for N0", [&]() {
      try {
        admin_client->sync_getNodesState(response, request);
        const auto& state = response.get_states()[0];
        const auto& shard_states = state.shard_states_ref().value();
        // We need to wait for all shard maintenances to finish
        bool all_shards_blocked = true;
        for (const auto& shard : shard_states) {
          if (shard.maintenance_ref().has_value()) {
            const auto& maintenance_progress = shard.maintenance_ref().value();
            if (maintenance_progress.get_status() !=
                MaintenanceStatus::REBUILDING_IS_BLOCKED) {
              all_shards_blocked = false;
            }
          }
        }
        return all_shards_blocked == true;
      } catch (thrift::NodeNotReady& e) {
        return false;
      }
      return false;
    });
  }

  ld_info("Rebuilding is blocked. Proceeding with unblocking...");

  thrift::MarkAllShardsUnrecoverableRequest request;
  request.set_user("test");
  request.set_reason("test");
  thrift::MarkAllShardsUnrecoverableResponse response;
  admin_client->sync_markAllShardsUnrecoverable(response, request);

  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, expected_shards, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
}

TEST_F(MaintenanceAPITest, RemoveNodesInMaintenance) {
  init();

  // Start with a disabled N5, to make the remove easier.
  cluster_->updateNodeAttributes(
      node_index_t(4), configuration::StorageState::DISABLED, 1, false);

  cluster_->start({0, 1, 2, 3});
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();

  {
    thrift::MaintenanceDefinition m1;
    m1.set_user("bunny");
    m1.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    m1.set_shards({mkShardID(4, -1)});
    m1.set_sequencer_nodes({mkNodeID(4)});
    m1.set_sequencer_target_state(SequencingState::DISABLED);

    thrift::MaintenanceDefinitionResponse m1_def;
    admin_client->sync_applyMaintenance(m1_def, m1);

    wait_until("N4 maintenance is completed", [&]() {
      admin_client->sync_applyMaintenance(m1_def, m1);
      return std::all_of(m1_def.get_maintenances().begin(),
                         m1_def.get_maintenances().end(),
                         [](const auto& m) {
                           return m.progress ==
                               thrift::MaintenanceProgress::COMPLETED;
                         });
    });
  }

  {
    // Apply another maintenance that includes N4
    thrift::MaintenanceDefinition m2;
    m2.set_user("bunny2");
    m2.set_shard_target_state(ShardOperationalState::DRAINED);
    m2.set_shards({mkShardID(4, -1), mkShardID(3, 0)});
    thrift::MaintenanceDefinitionResponse m2_def;
    admin_client->sync_applyMaintenance(m2_def, m2);
  }

  // Remove N5 from the config
  {
    thrift::NodesFilter fltr;
    fltr.set_node(mkNodeID(4));

    thrift::RemoveNodesRequest rem_req;
    rem_req.set_node_filters({fltr});

    thrift::RemoveNodesResponse resp;
    try {
      admin_client->sync_removeNodes(resp, rem_req);
    } catch (const thrift::ClusterMembershipOperationFailed& ex) {
      for (auto node : ex.get_failed_nodes()) {
        ld_error("N%d failed: %s",
                 *node.get_node_id().get_node_index(),
                 node.get_message().c_str());
      }
      FAIL();
    }

    wait_until("AdminServer's NC picks the removal", [&]() {
      thrift::NodesConfigResponse nodes_config;
      admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
      return nodes_config.version >= resp.new_nodes_configuration_version;
    });
  }

  // We expect to find only the second maintenance, with the removed shards not
  // there.
  thrift::MaintenancesFilter fltr;
  thrift::MaintenanceDefinitionResponse resp;
  admin_client->sync_getMaintenances(resp, fltr);
  auto output = resp.get_maintenances();
  EXPECT_EQ(1, output.size());
  EXPECT_THAT(resp.get_maintenances()[0].get_shards(),
              UnorderedElementsAre(mkShardID(3, 0)));
}
