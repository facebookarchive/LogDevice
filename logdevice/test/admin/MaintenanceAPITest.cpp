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
    // We expect this to be expanded into 1 maintenace group since everything
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
