/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;
using namespace facebook::logdevice::thrift;

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

  MaintenanceDefinitionResponse response;
  MaintenancesFilter filter;
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
  const size_t num_nodes = 5;
  const size_t num_shards = 2;

  Configuration::Nodes nodes;

  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(num_shards);
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);

  auto meta_configs =
      createMetaDataLogsConfig({0, 1, 2, 3, 4}, 2, NodeLocationScope::NODE);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(nodes)
          .enableSelfInitiatedRebuilding("1s")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--enable-safety-check-periodic-metadata-update", "true")
          .setParam("--disable-event-log-trimming", "true")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "0")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam(
              "--use-nodes-configuration-manager-nodes-configuration", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--loglevel", "debug")
          // Starts MaintenanceManager on N2
          .runMaintenanceManagerOn(maintenance_leader)
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
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
    MaintenanceDefinition def;
    MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        admin_client->sync_applyMaintenance(resp, def), thrift::InvalidRequest);
  }
  {
    MaintenanceDefinition def;
    thrift::ShardSet shards;
    shards.push_back(mkShardID(1, -1));
    def.set_shards(shards);
    def.set_shard_target_state(ShardOperationalState::DRAINED);

    MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since user is not set for this maintenance
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
  }
  // Apply maintenance for a sequencer NodeID that is unset.
  {
    MaintenanceDefinition def;
    def.set_user("my-user");
    def.set_sequencer_nodes({thrift::NodeID()});
    def.set_sequencer_target_state(SequencingState::DISABLED);

    MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since sequencers have empty NodeID
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
  }
  // Apply maintenance for a ShardID that has only the shard part set.
  {
    MaintenanceDefinition def;
    def.set_user("my-user");
    thrift::NodeID node; // has nothing set.
    thrift::ShardID shard;
    shard.set_shard_index(-1);
    shard.set_node(node);
    def.set_shards({shard});
    def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);

    MaintenanceDefinitionResponse resp;
    ASSERT_THROW(
        // Will fail since sequencers have empty NodeID
        admin_client->sync_applyMaintenance(resp, def),
        thrift::InvalidRequest);
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
    MaintenanceDefinition request;
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
    MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const MaintenanceDefinition& result = output[0];
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
    MaintenancesFilter fltr;
    MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, fltr);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const MaintenanceDefinition& result = output[0];
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
    MaintenanceDefinition request;
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
    MaintenanceDefinitionResponse resp;
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
    MaintenancesFilter fltr;
    MaintenanceDefinitionResponse resp;
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
    MaintenanceDefinition request;
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
    MaintenanceDefinitionResponse resp;
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
  MaintenanceDefinition request;
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
  MaintenanceDefinitionResponse resp;
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

TEST_F(MaintenanceAPITest, RemoveMaintenancesInvalid) {
  init();
  cluster_->start();
  cluster_->waitUntilAllAvailable();
  auto admin_client = cluster_->getNode(maintenance_leader).createAdminClient();
  // Wait until the RSM has replayed
  cluster_->getNode(maintenance_leader).waitUntilMaintenanceRSMReady();
  // We simply can't delete with an empty filter;
  RemoveMaintenancesRequest request;
  RemoveMaintenancesResponse resp;
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
    MaintenanceDefinition request;
    request.set_user("bunny");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(1, -1)});
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(2), mkNodeID(3)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_group(false);
    // We expect this to be expanded into 3 maintenance groups
    MaintenanceDefinitionResponse resp;
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
    MaintenanceDefinition request;
    request.set_user("dummy");
    request.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    request.set_shards({mkShardID(3, -1)});
    request.set_sequencer_nodes({mkNodeID(1)});
    request.set_sequencer_target_state(SequencingState::DISABLED);
    // to validate we correctly respect the attributes
    request.set_group(true);
    MaintenanceDefinitionResponse resp;
    admin_client->sync_applyMaintenance(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    fourth_group = output[0].group_id_ref().value();
  }
  // Let's remove by a group_id. We will remove one of bunny's maintenances.
  {
    RemoveMaintenancesRequest request;
    MaintenancesFilter fltr;
    auto group_to_remove = bunny_group_ids.back();
    bunny_group_ids.pop_back();
    fltr.set_group_ids({group_to_remove});
    request.set_filter(fltr);

    RemoveMaintenancesResponse resp;
    admin_client->sync_removeMaintenances(resp, request);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    ASSERT_EQ(group_to_remove, output[0].group_id_ref().value());
    // validate by getMaintenances.
    thrift::MaintenancesFilter req;
    MaintenanceDefinitionResponse response;
    admin_client->sync_getMaintenances(response, req);
    ASSERT_EQ(3, response.get_maintenances().size());
    for (const auto& def : response.get_maintenances()) {
      ASSERT_NE(group_to_remove, def.group_id_ref().value());
    }
  }
  // Let's remove all bunny's maintenances.
  {
    RemoveMaintenancesRequest request;
    MaintenancesFilter fltr;
    fltr.set_user("bunny");
    request.set_filter(fltr);
    RemoveMaintenancesResponse resp;
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
    MaintenanceDefinitionResponse response;
    admin_client->sync_getMaintenances(response, req);
    ASSERT_EQ(1, response.get_maintenances().size());
    ASSERT_EQ(
        fourth_group, response.get_maintenances()[0].group_id_ref().value());
  }
}

TEST_F(MaintenanceAPITest, getNodeState) {
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
        if (ServiceState::ALIVE == state.get_daemon_state()) {
          return true;
        }
      }
      return false;
    } catch (thrift::NodeNotReady& e) {
      return false;
    }
  });

  {
    NodesStateRequest request;
    NodesStateResponse response;
    admin_client->sync_getNodesState(response, request);
    ASSERT_EQ(5, response.get_states().size());
    for (const auto& state : response.get_states()) {
      ASSERT_EQ(ServiceState::ALIVE, state.get_daemon_state());
      const SequencerState& seq_state = state.sequencer_state_ref().value();
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
  // Let's apply a maintenance on node (1)
  std::string group_id;
  {
    MaintenanceDefinition maintenance1;
    maintenance1.set_user("dummy");
    maintenance1.set_shard_target_state(ShardOperationalState::DRAINED);
    // expands to all shards of node 1
    maintenance1.set_shards({mkShardID(1, -1)});
    maintenance1.set_sequencer_nodes({mkNodeID(1)});
    maintenance1.set_sequencer_target_state(SequencingState::DISABLED);
    MaintenanceDefinitionResponse created;
    admin_client->sync_applyMaintenance(created, maintenance1);
    ASSERT_EQ(1, created.get_maintenances().size());
    group_id = created.get_maintenances()[0].group_id_ref().value();
  }
  // Let's wait until the maintenance is applied.
  {
    NodesFilter filter;
    thrift::NodeID node_id;
    node_id.set_node_index(1);
    filter.set_node(node_id);
    NodesStateRequest request;
    request.set_filter(filter);
    NodesStateResponse response;
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
    ASSERT_EQ(ServiceState::ALIVE, state.get_daemon_state());
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
    MaintenancesFilter fltr;
    MaintenanceDefinitionResponse resp;
    admin_client->sync_getMaintenances(resp, fltr);
    auto output = resp.get_maintenances();
    ASSERT_EQ(1, output.size());
    const MaintenanceDefinition& result = output[0];
    ASSERT_EQ(thrift::MaintenanceProgress::COMPLETED, result.get_progress());
  }
}
