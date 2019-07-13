/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/include/Client.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class LocalLogStoreIntegrationTest : public IntegrationTestBase {};

// Replace a node keeping the data. Check that it starts.
TEST_F(LocalLogStoreIntegrationTest, ClusterMarkerAccept) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(3);

  // Only applies to new logdeviced instances -  after replacing nodes.
  cluster->setParam("ignore-cluster-marker", "false");

  std::string old_path = cluster->getNode(1).getDatabasePath();
  ASSERT_EQ(0, cluster->replace(1, true));
  std::string new_path = cluster->getNode(1).getDatabasePath();
  rename(old_path.c_str(), new_path.c_str());
  cluster->getNode(1).start();
  cluster->getNode(1).waitUntilStarted();
}

// Move DB from one node to another, check that server refuses to start.
TEST_F(LocalLogStoreIntegrationTest, ClusterMarkerReject) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(3);

  // Only applies to new logdeviced instances -  after replacing nodes.
  cluster->setParam("ignore-cluster-marker", "false");

  std::string old_path = cluster->getNode(1).getDatabasePath();
  ASSERT_EQ(0, cluster->replace(1, true));
  ASSERT_EQ(0, cluster->replace(2, true));
  std::string new_path = cluster->getNode(2).getDatabasePath();
  rename(old_path.c_str(), new_path.c_str());
  cluster->getNode(2).start();
  int rv = cluster->getNode(2).waitUntilExited();
  ASSERT_NE(0, rv);
}

// Check that servers can start with a corrupt DB and that the cluster can
// still properly function with some nodes crippled.
TEST_F(LocalLogStoreIntegrationTest, StartWithCorruptDB) {
  const int NNODES = 4;

  // Custom nodes config where all generations of the last two nodes are 2,
  // so that they can start rebuilding (but shouldn't because of metadata).
  Configuration::Nodes nodes_config;
  for (int i = 0; i < NNODES; ++i) {
    Configuration::Node node;
    if (i == 0) {
      node.addSequencerRole();
    } else {
      node.addStorageRole(/*num_shards*/ 4);
    }
    node.generation = i < NNODES - 2 ? 1 : 2;
    nodes_config[i] = std::move(node);
  }

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNodes(nodes_config)
                     .setNumDBShards(4)
                     .deferStart()
                     .create(NNODES);

  // Cripple two storage nodes.  On one node, corrupt half of the DB
  // instances.  On the other, corrupt the other half.  Writes to all logs
  // should still succeed as records for every log can still be placed on two
  // different hosts.  However, if the crippled nodes fail to start and
  // process writes for the healthy DBs, the test will fail.
  for (int idx = NNODES - 2; idx < NNODES; ++idx) {
    ld_check(cluster->getConfig()
                 ->get()
                 ->serverConfig()
                 ->getNode(idx)
                 ->isReadableStorageNode());
    IntegrationTestUtils::Node& node = cluster->getNode(idx);

    std::vector<uint32_t> shards_to_corrupt;
    auto sharded_store = node.createLocalLogStore();
    for (int i = 0; i < sharded_store->numShards(); ++i) {
      RocksDBLogStoreBase* store =
          dynamic_cast<RocksDBLogStoreBase*>(sharded_store->getByIndex(i));
      ld_check(store != nullptr);
      if (i % 2 == idx % 2) {
        shards_to_corrupt.push_back(i);
      } else {
        RebuildingCompleteMetadata meta;
        EXPECT_EQ(0, store->writeStoreMetadata(meta));
      }
    }

    node.corruptShards(shards_to_corrupt, std::move(sharded_store));
  }

  ASSERT_EQ(0, cluster->start());

  for (int idx = NNODES - 2; idx < NNODES; ++idx) {
    EXPECT_EQ(2, cluster->getNode(idx).stats()["failing_log_stores"]);
  }

  char data[128]; // send the contents of this array as payload
  const Payload payload(data, sizeof data);

  std::shared_ptr<Client> client = cluster->createClient();
  for (logid_t log_id(1); log_id.val_ <= 2; ++log_id.val_) {
    for (int i = 0; i < 20; ++i) {
      lsn_t lsn = client->appendSync(log_id, payload);
      ASSERT_NE(lsn, LSN_INVALID);
    }
  }
}

TEST_F(LocalLogStoreIntegrationTest, IOTracingSmokeTest) {
  // Run a cluster with IO tracing enabled and check that it doesn't crash.
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--rocksdb-io-tracing-shards", "0,1")
                     .create(1);

  // Write a record.
  auto client = cluster->createClient();
  lsn_t lsn = client->appendSync(logid_t(1), "hi");
  EXPECT_NE(LSN_INVALID, lsn);

  // Read the record back.
  auto read = [&] {
    auto reader = client->createReader(1);
    reader->startReading(logid_t(1), LSN_OLDEST);
    lsn_t cur_lsn = LSN_OLDEST;
    while (true) {
      std::vector<std::unique_ptr<DataRecord>> recs;
      GapRecord gap;
      ssize_t rv = reader->read(1, &recs, &gap);
      if (rv == 1) {
        EXPECT_EQ(cur_lsn, recs[0]->attrs.lsn);
        EXPECT_EQ(lsn, recs[0]->attrs.lsn);
        EXPECT_EQ("hi", recs[0]->payload.toString());
        break;
      } else {
        ASSERT_EQ(-1, rv);
        EXPECT_EQ(cur_lsn, gap.lo);
        EXPECT_GE(gap.hi, gap.lo);
        EXPECT_LT(gap.hi, lsn);
        cur_lsn = gap.hi + 1;
      }
    }
  };

  read();

  // Restart the node.
  cluster->getNode(0).restart(
      /* graceful */ true, /* wait_until_available */ false);

  // Read again, expecting to hit disk.
  read();
}
