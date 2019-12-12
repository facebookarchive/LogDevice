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
#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/ops/utils/MaintenanceManagerUtils.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

#define LOG_ID logid_t(1)

class MaintenanceManagerMigrationTest : public IntegrationTestBase {
 public:
  void init();
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;

 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();
  }
};

void MaintenanceManagerMigrationTest::init() {
  const size_t num_nodes = 6;
  const size_t num_shards = 2;

  Configuration::Nodes nodes;

  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(num_shards);
  }

  auto log_attrs = logsconfig::LogAttributes().with_replicationFactor(3);

  auto meta_configs =
      createMetaDataLogsConfig({0, 1, 2, 3, 4, 5}, 3, NodeLocationScope::NODE);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(nodes)
          .setNodesConfigurationSourceOfTruth(
              IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
          .enableSelfInitiatedRebuilding("10s")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--disable-event-log-trimming", "true")
          .setParam("--enable-self-initiated-rebuilding", "false")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "0")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--loglevel", "debug")
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
          .setMetaDataLogsConfig(meta_configs)
          .deferStart()
          .create(num_nodes);
}

TEST_F(MaintenanceManagerMigrationTest, Migration) {
  init();
  cluster_->start();

  auto seed_addr = "data:" + cluster_->getNode(0).addrs_.protocol.toString();
  auto settings = std::unique_ptr<ClientSettings>(ClientSettings::create());
  settings->set("enable-nodes-configuration-manager", "true");
  settings->set("nodes-configuration-seed-servers", seed_addr);
  settings->set("admin-client-capabilities", "true");
  settings->set("nodes-configuration-file-store-dir", cluster_->getNCSPath());

  std::shared_ptr<Client> client =
      cluster_->createClient(getDefaultTestTimeout(), std::move(settings));
  auto clientImpl = dynamic_cast<ClientImpl*>(client.get());
  clientImpl->allowWriteInternalLog();

  // Kill N1
  cluster_->getNode(1).kill();
  // Add N1:S0 to rebuilding set
  IntegrationTestUtils::requestShardRebuilding(*client, 1, 0);
  // Add N1:S1 to rebuilding set
  IntegrationTestUtils::requestShardRebuilding(*client, 1, 1);
  // Wait for it to show up in EventLog
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(1, 0), ShardID(1, 1)},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  // Run the migration now
  auto status = migrateToMaintenanceManager(*client);
  ld_check(status == E::OK);

  // N1's internal maintenance should show up in Maintenance log
  // Start MaintenanceManager on N5
  cluster_->getNode(5).kill();
  cluster_->getNode(5).setParam("--enable-maintenance-manager", "true");
  cluster_->getNode(5).start();
  cluster_->getNode(5).waitUntilMaintenanceRSMReady();
  auto admin_client = cluster_->getNode(5).createAdminClient();
  {
    thrift::MaintenanceDefinitionResponse resp;
    thrift::MaintenancesFilter filter;
    admin_client->sync_getMaintenances(resp, filter);
    auto output = resp.get_maintenances();
    ASSERT_EQ(2, output.size());
    ASSERT_EQ(output[0].user, maintenance::INTERNAL_USER);
    ASSERT_EQ(output[1].user, maintenance::INTERNAL_USER);
    thrift::ShardSet res;
    res.push_back(std::move(output[0].shards[0]));
    res.push_back(std::move(output[1].shards[0]));
    ASSERT_THAT(res, UnorderedElementsAre(mkShardID(1, 0), mkShardID(1, 1)));
  }

  // this is to prevent MM from aborting rebuilding. This is essentially
  // simulating a scenario where one has to run migration multiple times
  // for the maintenances and existing rebuilding set to converge.
  // See the notes in the MaintenanceManagerUtils.h
  cluster_->getNode(5).kill();
  cluster_->getNode(5).setParam("--enable-maintenance-manager", "false");
  cluster_->getNode(5).start();

  // Start draining N2
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  IntegrationTestUtils::requestShardRebuilding(*client, 2, 0, flags);
  IntegrationTestUtils::requestShardRebuilding(*client, 2, 1, flags);
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(2, 0), ShardID(2, 1)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      false);

  // Run the migration now
  status = migrateToMaintenanceManager(*client);
  ld_check(status == E::OK);

  // N2's external maintenance should show up in Maintenance log
  cluster_->getNode(5).kill();
  cluster_->getNode(5).setParam("--enable-maintenance-manager", "true");
  cluster_->getNode(5).start();
  cluster_->getNode(5).waitUntilMaintenanceRSMReady();
  {
    admin_client = cluster_->getNode(5).createAdminClient();
    thrift::MaintenanceDefinitionResponse resp;
    thrift::MaintenancesFilter filter;
    filter.set_user(maintenance::MIGRATION_USER);
    admin_client->sync_getMaintenances(resp, filter);
    auto output = resp.get_maintenances();
    ASSERT_EQ(2, output.size());
    thrift::ShardSet res;
    res.push_back(std::move(output[0].shards[0]));
    res.push_back(std::move(output[1].shards[0]));
    ASSERT_THAT(res, UnorderedElementsAre(mkShardID(2, 0), mkShardID(2, 1)));
  }
}
