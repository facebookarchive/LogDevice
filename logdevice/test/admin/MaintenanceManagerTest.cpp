/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManager.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

#define LOG_ID logid_t(1)

class MaintenanceManagerTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*filter relocate shards*/> {
 public:
  void init();
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;

 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();
  }
};

void MaintenanceManagerTest::init() {
  const size_t num_nodes = 5;
  const size_t num_shards = 2;

  auto nodes_configuration =
      createSimpleNodesConfig(num_nodes, num_shards, true, 2);

  auto log_attrs = logsconfig::LogAttributes().with_replicationFactor(2);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(std::move(nodes_configuration))
          .setNodesConfigurationSourceOfTruth(
              IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
          .enableSelfInitiatedRebuilding("10s")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--disable-event-log-trimming", "true")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "1")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--filter-relocate-shards", GetParam() ? "true" : "false")
          .setParam("--loglevel", "debug")
          .useStandaloneAdminServer(true)
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
          .deferStart()
          .create(num_nodes);
}

static lsn_t write_test_records(std::shared_ptr<Client> client,
                                logid_t logid,
                                size_t num_records) {
  lsn_t first_lsn = LSN_INVALID;
  static size_t counter = 0;
  for (size_t i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(++counter));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn)
        << "Append failed (E::" << error_name(err) << ")";
    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }
  }
  return first_lsn;
}

TEST_P(MaintenanceManagerTest, BasicDrain) {
  init();
  cluster_->start({0, 1, 2, 3, 4});
  cluster_->waitUntilAllStartedAndPropagatedInGossip();
  std::shared_ptr<Client> client = cluster_->createClient();
  write_test_records(client, LOG_ID, 10);

  auto& processor = static_cast<ClientImpl*>(client.get())->getProcessor();
  auto maintenanceLogWriter =
      std::make_unique<MaintenanceLogWriter>(&processor);

  // Add a maintenance to drain N3
  thrift::MaintenanceDefinition def;
  def.set_shards({mkShardID(3, -1)});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user("Test");
  def.set_reason("Integration Test");
  def.set_skip_safety_checks(false);
  def.set_force_restore_rebuilding(false);
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(false);
  def.set_group_id("N3S-1");
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());

  // Add another MAY_DISAPPEAR maintenance to N3, still, drain should win.
  thrift::MaintenanceDefinition def2;
  def2.set_shards({mkShardID(3, -1)});
  def2.set_shard_target_state(thrift::ShardOperationalState::MAY_DISAPPEAR);
  def2.set_user("Test-maydisappear");
  def2.set_reason("Integration Test");
  def2.set_skip_safety_checks(false);
  def2.set_force_restore_rebuilding(false);
  def2.set_group(true);
  def2.set_ttl_seconds(0);
  def2.set_allow_passive_drains(false);
  def2.set_group_id("N3S-1-md");
  def2.set_created_on(SystemTimestamp::now().toMilliseconds().count());

  auto maintenanceDelta = std::make_unique<MaintenanceDelta>();
  maintenanceDelta->set_apply_maintenances({def, def2});
  write_to_maintenance_log(*client, *maintenanceDelta);
  auto admin_client = cluster_->getAdminServer()->createAdminClient();
  wait_until("ShardOperationalState is DRAINED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::DRAINED;
  });

  // Now remove the DRAINED maintenance, node should go back to being
  // MAY_DISAPEAR
  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  thrift::MaintenancesFilter filter;
  filter.set_group_ids({"N3S-1"});
  filter.set_user("Test");

  thrift::RemoveMaintenancesRequest req;
  req.set_filter(std::move(filter));
  req.set_user("Test");
  req.set_reason("Integration Test");

  maintenanceDelta->set_remove_maintenances(std::move(req));
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is MAY_DISAPPEAR", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::MAY_DISAPPEAR;
  });

  // Now remove the MAY_DISAPPEAR maintenance, node should go back to being
  // ENABLED
  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  filter.set_group_ids({"N3S-1-md"});
  filter.set_user("Test-maydisappear");

  req.set_filter(std::move(filter));
  req.set_user("Test-maydisappear");
  req.set_reason("Integration Test");

  maintenanceDelta->set_remove_maintenances(std::move(req));
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });

  // Kill N3 now, internal maintenance should be added and node eventually
  // set to DRAINED
  cluster_->getNode(3).kill();
  wait_until("ShardOperationalState is DRAINED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::DRAINED;
  });

  // Start N3. It shoudl ack its rebuilding and remove the maintenance
  cluster_->getNode(3).start();
  cluster_->getNode(3).waitUntilAvailable();
  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });
}

TEST_P(MaintenanceManagerTest, BasicPassiveDrain) {
  init();
  cluster_->start({0, 1, 2, 3});
  cluster_->waitUntilAllStartedAndPropagatedInGossip();
  std::shared_ptr<Client> client = cluster_->createClient();
  write_test_records(client, LOG_ID, 10);

  auto& processor = static_cast<ClientImpl*>(client.get())->getProcessor();
  auto maintenanceLogWriter =
      std::make_unique<MaintenanceLogWriter>(&processor);

  // Add a maintenance to passive drain everything
  thrift::MaintenanceDefinition def;
  def.set_shards({mkShardID(0, -1), mkShardID(2, -1)});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user("Test");
  def.set_reason("Integration Test");
  def.set_skip_safety_checks(false);
  def.set_force_restore_rebuilding(false);
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(true);
  def.set_group_id("PSV-1");
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());

  auto maintenanceDelta = std::make_unique<MaintenanceDelta>();
  maintenanceDelta->set_apply_maintenances({def});
  write_to_maintenance_log(*client, *maintenanceDelta);

  auto admin_client = cluster_->getAdminServer()->createAdminClient();

  wait_until("ShardOperationalState is PASSIVE_DRAINING", [&]() {
    auto state = get_shard_operational_state(*admin_client, 0, 0);
    return state == thrift::ShardOperationalState::PASSIVE_DRAINING;
  });

  // Now remove the DRAINED maintenance, node should go back to being
  // MAY_DISAPEAR
  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  thrift::MaintenancesFilter filter;
  filter.set_group_ids({"PSV-1"});
  filter.set_user("Test");

  thrift::RemoveMaintenancesRequest req;
  req.set_filter(std::move(filter));
  req.set_user("Test");
  req.set_reason("Integration Test");

  maintenanceDelta->set_remove_maintenances(std::move(req));
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 0, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });
}

TEST_P(MaintenanceManagerTest, Snapshotting) {
  const size_t num_nodes = 5;
  const size_t num_shards = 2;

  auto nodes_configuration =
      createSimpleNodesConfig(num_nodes, num_shards, true, 2);

  auto log_attrs = logsconfig::LogAttributes().with_replicationFactor(2);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(std::move(nodes_configuration))
          .enableSelfInitiatedRebuilding("10s")
          .setNodesConfigurationSourceOfTruth(
              IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--disable-event-log-trimming", "true")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "0")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam("--max-unavailable-storage-capacity-pct", "50")
          .setParam("--max-unavailable-sequencing-capacity-pct", "50")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          // Note that this means that all nodes will be taking snapshots. This
          // is still safe and makes the test easier since we can ask any node
          // on whether it has taken a snapshot or not. Ideally, this test
          // should not exist and the snapshotting testing should be on lower
          // level, this is leaking the whole abstraction of the RSM.
          .setParam("--maintenance-log-snapshotting", "true")
          .setParam("--maintenance-log-max-delta-records", "2")
          .setParam("--loglevel", "debug")
          .useStandaloneAdminServer(true)
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
          .deferStart()
          .create(num_nodes);

  cluster_->start({0, 1, 2, 3, 4});
  cluster_->waitUntilAllStartedAndPropagatedInGossip();
  std::shared_ptr<Client> client = cluster_->createClient();
  write_test_records(client, LOG_ID, 10);

  auto& processor = static_cast<ClientImpl*>(client.get())->getProcessor();
  auto maintenanceLogWriter =
      std::make_unique<MaintenanceLogWriter>(&processor);

  // Add a maintenance to drain N3
  thrift::MaintenanceDefinition def;
  def.set_shards({mkShardID(3, -1)});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user("Test");
  def.set_reason("Integration Test");
  def.set_skip_safety_checks(false);
  def.set_force_restore_rebuilding(false);
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(false);
  def.set_group_id("N3S-1");
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());

  auto maintenanceDelta = std::make_unique<MaintenanceDelta>();
  maintenanceDelta->set_apply_maintenances({def});
  write_to_maintenance_log(*client, *maintenanceDelta);

  auto admin_client = cluster_->getAdminServer()->createAdminClient();

  wait_until("N3's ShardOperationalState is DRAINED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::DRAINED;
  });

  auto stats = cluster_->getNode(2).stats();
  EXPECT_EQ(stats["maintenance_log_snapshot_size"], 0);

  // Add another MAY_DISAPPEAR maintenance to N2
  thrift::MaintenanceDefinition def2;
  def2.set_shards({mkShardID(2, -1)});
  def2.set_shard_target_state(thrift::ShardOperationalState::MAY_DISAPPEAR);
  def2.set_user("Test-maydisappear");
  def2.set_reason("Integration Test");
  def2.set_skip_safety_checks(false);
  def2.set_force_restore_rebuilding(false);
  def2.set_group(true);
  def2.set_ttl_seconds(0);
  def2.set_allow_passive_drains(false);
  def2.set_group_id("N2S-1-md");
  def2.set_created_on(SystemTimestamp::now().toMilliseconds().count());

  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  maintenanceDelta->set_apply_maintenances({def2});
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("N2's ShardOperationalState is MAY_DISAPPEAR", [&]() {
    auto state = get_shard_operational_state(*admin_client, 2, 0);
    return state == thrift::ShardOperationalState::MAY_DISAPPEAR;
  });

  // Since 2 records have now been written we should have snapshotted
  wait_until("maintenance_log_snapshot_size is >0", [&]() {
    stats = cluster_->getNode(2).stats();
    return stats["maintenance_log_snapshot_size"] > 0;
  });

  // Now remove the MAY_DISAPPEAR maintenance, node should go back to being
  // ENABLED
  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  thrift::MaintenancesFilter filter;
  filter.set_group_ids({"N2S-1-md"});
  filter.set_user("Test-maydisappear");

  thrift::RemoveMaintenancesRequest req;
  req.set_filter(std::move(filter));
  req.set_user("Test-maydisappear");
  req.set_reason("Integration Test");

  maintenanceDelta->set_remove_maintenances(std::move(req));
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("N2's ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 2, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });

  // Now remove the DRAIN maintenance on N3
  maintenanceDelta = std::make_unique<MaintenanceDelta>();
  thrift::MaintenancesFilter filter2;
  filter2.set_group_ids({"N3S-1"});
  filter2.set_user("Test");

  thrift::RemoveMaintenancesRequest req2;
  req2.set_filter(std::move(filter2));
  req2.set_user("Test");
  req2.set_reason("Integration Test");
  maintenanceDelta->set_remove_maintenances(std::move(req2));
  write_to_maintenance_log(*client, *maintenanceDelta);

  wait_until("N3's ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 3, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });
}

TEST_P(MaintenanceManagerTest, RestoreDowngradedToTimeRangeRebuilding) {
  const size_t num_nodes = 6;
  const size_t num_shards = 2;

  auto nodes_configuration =
      createSimpleNodesConfig(num_nodes, num_shards, true, 2);

  auto data_log_attrs = logsconfig::LogAttributes().with_replicationFactor(5);

  auto log_attrs = logsconfig::LogAttributes()
                       .with_replicationFactor(2)
                       .with_extraCopies(0)
                       .with_syncedCopies(0)
                       .with_maxWritesInFlight(2048);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(std::move(nodes_configuration))
          .enableSelfInitiatedRebuilding("10s")
          .setNodesConfigurationSourceOfTruth(
              IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
          .setParam("--rebuild-store-durability", "async_write")
          .setParam("--append-store-durability", "memory")
          .setParam("--rebuilding-restarts-grace-period", "10s")
          .setParam("--rocksdb-partition-data-age-flush-trigger", "900s")
          .setParam("--rocksdb-partition-idle-flush-trigger", "900s")
          .setParam("--rocksdb-min-manual-flush-interval", "900s")
          .setParam("--rocksdb-partition-duration", "900s")
          .setParam("--rocksdb-ld-managed-flushes", "true")
          .setParam("--rocksdb-partition-timestamp-granularity", "100ms")
          .setParam("--sticky-copysets-block-size", "10")
          .setParam("--event-log-grace-period", "1ms")
          .setParam("--disable-event-log-trimming", "true")
          .useHashBasedSequencerAssignment()
          .setParam("--min-gossips-for-stable-state", "0")
          .setParam("--enable-cluster-maintenance-state-machine", "true")
          .setParam("--enable-nodes-configuration-manager", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--loglevel", "debug")
          .setParam("--max-node-rebuilding-percentage", "80")
          .setLogAttributes(data_log_attrs)
          .useStandaloneAdminServer(true)
          .setNumDBShards(num_shards)
          .setConfigLogAttributes(log_attrs)
          .setMaintenanceLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setLogGroupName("test_logrange")
          .deferStart()
          .create(num_nodes);

  NodeSetIndices node_set(6);
  std::iota(node_set.begin(), node_set.end(), 0);

  cluster_->start(node_set);
  cluster_->waitUntilAllStartedAndPropagatedInGossip();
  std::shared_ptr<Client> client = cluster_->createClient();
  write_test_records(client, LOG_ID, 100);

  auto& processor = static_cast<ClientImpl*>(client.get())->getProcessor();
  auto maintenanceLogWriter =
      std::make_unique<MaintenanceLogWriter>(&processor);

  // Kill N1 now, internal maintenance should be added and node eventually
  // set to DRAINED
  cluster_->getNode(1).kill();

  auto admin_client = cluster_->getAdminServer()->createAdminClient();
  wait_until("ShardOperationalState is MIGRATING_DATA", [&]() {
    auto state = get_shard_operational_state(*admin_client, 1, 0);
    return state == thrift::ShardOperationalState::MIGRATING_DATA;
  });

  // Start the nodes back again. They should remove the internal maintenance
  // and trigger TRR
  cluster_->getNode(1).start();

  cluster_->getNode(1).waitUntilAvailable();
  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 1, 0);
    return state == thrift::ShardOperationalState::ENABLED;
  });

  // Verify that the shards are dirty
  EXPECT_FALSE(cluster_->getNode(1).dirtyShardInfo().empty());

  // Add a maintenance to drain N1
  thrift::MaintenanceDefinition def;
  def.set_shards({mkShardID(1, -1)});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user("Test");
  def.set_reason("Integration Test");
  def.set_skip_safety_checks(false);
  def.set_force_restore_rebuilding(false);
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(false);
  def.set_group_id("N1S-1");
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());
  auto maintenanceDelta = std::make_unique<MaintenanceDelta>();
  maintenanceDelta->set_apply_maintenances({def});
  write_to_maintenance_log(*client, *maintenanceDelta);

  // Wait until maintenance completes
  wait_until("ShardOperationalState is DRAINED", [&]() {
    auto state = get_shard_operational_state(*admin_client, 1, 0);
    return state == thrift::ShardOperationalState::DRAINED;
  });
}

INSTANTIATE_TEST_CASE_P(MaintenanceManagerTest,
                        MaintenanceManagerTest,
                        ::testing::Bool());
