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
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

#define LOG_ID logid_t(1)

class MaintenanceManagerTest : public IntegrationTestBase {
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
          .runMaintenanceManagerOn(2)
          .setNumDBShards(num_shards)
          .setLogGroupName("test_logrange")
          .setLogAttributes(log_attrs)
          .setMetaDataLogsConfig(meta_configs)
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

static lsn_t writeToMaintenanceLog(Client& client,
                                   maintenance::MaintenanceDelta& delta) {
  logid_t maintenance_log_id =
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS;

  // Retry for at most 30s to avoid test failures due to transient failures
  // writing to the maintenance log.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};

  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

  lsn_t lsn = LSN_INVALID;
  auto clientImpl = dynamic_cast<ClientImpl*>(&client);
  clientImpl->allowWriteInternalLog();
  auto rv = wait_until("writes to the maintenance log succeed",
                       [&]() {
                         lsn = clientImpl->appendSync(
                             maintenance_log_id, serializedData);
                         return lsn != LSN_INVALID;
                       },
                       deadline);

  if (rv != 0) {
    ld_check(lsn == LSN_INVALID);
    ld_error("Could not write delta in maintenance log(%lu): %s(%s)",
             maintenance_log_id.val_,
             error_name(err),
             error_description(err));
    return false;
  }

  ld_info(
      "Wrote maintenance log delta with lsn %s", lsn_to_string(lsn).c_str());
  return lsn;
}

TEST_F(MaintenanceManagerTest, BasicDrain) {
  init();
  cluster_->start({0, 1, 2, 3, 4});
  for (auto n : {0, 1, 2, 3, 4}) {
    cluster_->getNode(n).waitUntilAvailable();
  }
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
  writeToMaintenanceLog(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is DRAINED", [&]() {
    auto state = cluster_->getNode(2).sendCommand("info shardopstate 3 0");
    const std::string expected_text = "DRAINED\r\nEND\r\n";
    return state == expected_text;
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
  writeToMaintenanceLog(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is MAY_DISAPPEAR", [&]() {
    auto state = cluster_->getNode(2).sendCommand("info shardopstate 3 0");
    const std::string expected_text = "MAY_DISAPPEAR\r\nEND\r\n";
    return state == expected_text;
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
  writeToMaintenanceLog(*client, *maintenanceDelta);

  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = cluster_->getNode(2).sendCommand("info shardopstate 3 0");
    const std::string expected_text = "ENABLED\r\nEND\r\n";
    return state == expected_text;
  });

  // Kill N3 now, internal maintenance should be added and node eventually
  // set to DRAINED
  cluster_->getNode(3).kill();
  wait_until("ShardOperationalState is DRAINED", [&]() {
    auto state = cluster_->getNode(2).sendCommand("info shardopstate 3 0");
    const std::string expected_text = "DRAINED\r\nEND\r\n";
    return state == expected_text;
  });

  // Start N3. It shoudl ack its rebuilding and remove the maintenance
  cluster_->getNode(3).start();
  cluster_->getNode(3).waitUntilAvailable();
  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = cluster_->getNode(2).sendCommand("info shardopstate 3 0");
    const std::string expected_text = "ENABLED\r\nEND\r\n";
    return state == expected_text;
  });
}

TEST_F(MaintenanceManagerTest, RestoreDowngradedToTimeRangeRebuilding) {
  const size_t num_nodes = 5;
  const size_t num_shards = 2;

  Configuration::Nodes nodes;

  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(num_shards);
  }

  logsconfig::LogAttributes data_log_attrs;
  data_log_attrs.set_replicationFactor(5);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(2048);

  auto meta_configs =
      createMetaDataLogsConfig({0, 1, 2, 3, 4}, 2, NodeLocationScope::NODE);

  cluster_ =
      IntegrationTestUtils::ClusterFactory()
          .setNumLogs(1)
          .setNodes(nodes)
          .enableSelfInitiatedRebuilding("1s")
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
              "--use-nodes-configuration-manager-nodes-configuration", "true")
          .setParam(
              "--nodes-configuration-manager-intermediary-shard-state-timeout",
              "2s")
          .setParam("--loglevel", "debug")
          .setParam("--max-node-rebuilding-percentage", "80")
          .setLogAttributes(data_log_attrs)
          // Starts MaintenanceManager on N2
          .runMaintenanceManagerOn(3)
          .setNumDBShards(num_shards)
          .setConfigLogAttributes(log_attrs)
          .setMaintenanceLogAttributes(log_attrs)
          .setEventLogAttributes(log_attrs)
          .setLogGroupName("test_logrange")
          .setMetaDataLogsConfig(meta_configs)
          .deferStart()
          .create(num_nodes);

  NodeSetIndices node_set(5);
  std::iota(node_set.begin(), node_set.end(), 0);

  cluster_->start(node_set);
  for (auto n : node_set) {
    cluster_->getNode(n).waitUntilAvailable();
  }
  std::shared_ptr<Client> client = cluster_->createClient();
  write_test_records(client, LOG_ID, 100);

  auto& processor = static_cast<ClientImpl*>(client.get())->getProcessor();
  auto maintenanceLogWriter =
      std::make_unique<MaintenanceLogWriter>(&processor);

  // Kill N1 now, internal maintenance should be added and node eventually
  // set to DRAINED
  cluster_->getNode(1).kill();

  wait_until("ShardOperationalState is MIGRATING_DATA", [&]() {
    auto state = cluster_->getNode(3).sendCommand("info shardopstate 1 0");
    const std::string expected_text = "MIGRATING_DATA\r\nEND\r\n";
    return state == expected_text;
  });

  // Start the nodes back again. They should remove the internal maintenance
  // and trigger TRR
  cluster_->getNode(1).start();

  cluster_->getNode(1).waitUntilAvailable();
  wait_until("ShardOperationalState is ENABLED", [&]() {
    auto state = cluster_->getNode(3).sendCommand("info shardopstate 1 0");
    const std::string expected_text = "ENABLED\r\nEND\r\n";
    return state == expected_text;
  });

  // Verify that the shards are dirty
  EXPECT_FALSE(cluster_->getNode(1).dirtyShardInfo().empty());
}
