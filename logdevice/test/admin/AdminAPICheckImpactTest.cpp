/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"

using namespace facebook::logdevice;
using namespace ::testing;

class AdminAPICheckImpactTest : public IntegrationTestBase {};

const logid_t LOG_ID{1};
const logid_t LOG_ID2{2};

namespace {
void write_test_records(std::shared_ptr<Client> client,
                        logid_t logid,
                        size_t num_records) {
  static size_t counter = 0;
  for (size_t i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(++counter));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn)
        << "Append failed (E::" << error_name(err) << ")";
  }
}
} // namespace

TEST_F(AdminAPICheckImpactTest, DisableReads) {
  const size_t num_nodes = 5;
  const size_t num_shards = 3;

  Configuration::Nodes nodes;

  for (int i = 0; i < num_nodes; ++i) {
    nodes[i].generation = 1;
    nodes[i].addSequencerRole();
    nodes[i].addStorageRole(num_shards);
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);

  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_singleWriter(false);
  internal_log_attrs.set_replicationFactor(3);
  internal_log_attrs.set_extraCopies(0);
  internal_log_attrs.set_syncedCopies(0);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNumLogs(2)
                     .setNodes(nodes)
                     // switches on gossip
                     .useHashBasedSequencerAssignment()
                     .setNumDBShards(num_shards)
                     .enableSelfInitiatedRebuilding()
                     .setLogAttributes(log_attrs)
                     .setEventLogAttributes(internal_log_attrs)
                     .setConfigLogAttributes(internal_log_attrs)
                     .create(num_nodes);

  for (const auto& it : cluster->getNodes()) {
    node_index_t idx = it.first;
    cluster->getNode(idx).waitUntilAvailable();
  }

  ShardAuthoritativeStatusMap shard_status{LSN_INVALID};
  int rv = cluster->getShardAuthoritativeStatusMap(shard_status);
  ASSERT_EQ(0, rv);
  std::shared_ptr<Client> client = cluster->createClient();
  ld_info("Waiting for all nodes to acknowledge rebuilding...");
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {ShardID(0, 1),
       ShardID(1, 1),
       ShardID(2, 1),
       ShardID(3, 1),
       ShardID(4, 1)},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);

  write_test_records(client, LOG_ID, 10);
  write_test_records(client, LOG_ID2, 10);

  ld_info("Waiting for metadata log writes to complete");
  cluster->waitForMetaDataLogWrites();

  thrift::ShardSet shards;
  for (int i = 0; i < num_nodes; ++i) {
    thrift::ShardID shard;
    thrift::NodeID node;
    node.set_node_index(i);
    shard.set_node(node);
    shard.set_shard_index(0);
    shards.push_back(std::move(shard));
  }

  cluster->getNode(1).waitUntilNodeStateReady();
  auto admin_client = cluster->getNode(1).createAdminClient();
  ASSERT_NE(nullptr, admin_client);
  // Check Impact
  thrift::CheckImpactRequest request;
  thrift::CheckImpactResponse response;
  request.set_shards(shards);
  request.set_target_storage_state(thrift::ShardStorageState::DISABLED);
  admin_client->sync_checkImpact(response, request);

  ASSERT_TRUE(*response.get_internal_logs_affected());
  ASSERT_THAT(
      response.get_impact(),
      UnorderedElementsAre(thrift::OperationImpact::READ_AVAILABILITY_LOSS,
                           thrift::OperationImpact::WRITE_AVAILABILITY_LOSS));
  ASSERT_FALSE(response.get_logs_affected()->empty());
  ASSERT_EQ(3, response.get_logs_affected()->size());
  for (auto impact_on_epoch : *response.get_logs_affected()) {
    thrift::ReplicationProperty repl;
    repl[thrift::LocationScope::NODE] = 3;
    if (impact_on_epoch.log_id == 0) {
      // Metadata Logs.
      ASSERT_EQ(0, impact_on_epoch.epoch);
      ASSERT_EQ(repl, impact_on_epoch.replication);
      ASSERT_THAT(impact_on_epoch.get_impact(),
                  UnorderedElementsAre(
                      thrift::OperationImpact::READ_AVAILABILITY_LOSS,
                      thrift::OperationImpact::WRITE_AVAILABILITY_LOSS));

    } else if (impact_on_epoch.log_id == 4611686018427387903) {
      ASSERT_EQ(4611686018427387903, impact_on_epoch.log_id);
      ASSERT_EQ(1, impact_on_epoch.epoch);
      ASSERT_EQ(repl, impact_on_epoch.replication);
      ASSERT_EQ(shards, impact_on_epoch.storage_set);
      ASSERT_THAT(
          impact_on_epoch.get_impact(),
          UnorderedElementsAre(thrift::OperationImpact::READ_AVAILABILITY_LOSS,
                               thrift::OperationImpact::REBUILDING_STALL));
    } else if (impact_on_epoch.log_id == 4611686018427387900) {
      ASSERT_EQ(1, impact_on_epoch.epoch);
      ASSERT_EQ(shards, impact_on_epoch.storage_set);
      ASSERT_EQ(repl, impact_on_epoch.replication);
      ASSERT_THAT(
          impact_on_epoch.get_impact(),
          UnorderedElementsAre(thrift::OperationImpact::READ_AVAILABILITY_LOSS,
                               thrift::OperationImpact::REBUILDING_STALL));
      ASSERT_TRUE(impact_on_epoch.get_storage_set_metadata());
      ASSERT_EQ(
          shards.size(), impact_on_epoch.get_storage_set_metadata()->size());
      thrift::StorageSetMetadata* storage_set_metadata =
          impact_on_epoch.get_storage_set_metadata();
      ASSERT_EQ(shards.size(), storage_set_metadata->size());
      thrift::ShardMetadata& shard1 = storage_set_metadata->at(0);
      ASSERT_EQ(thrift::ShardDataHealth::HEALTHY, shard1.get_data_health());
      ASSERT_TRUE(shard1.get_is_alive());
      ASSERT_EQ(
          thrift::ShardStorageState::READ_WRITE, shard1.get_storage_state());
      ASSERT_FALSE(shard1.get_location());
    }
  }
}
