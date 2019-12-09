/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingMarkerChecker.h"

#include <gtest/gtest.h>

#include "logdevice/common/test/MockNodesConfigurationManager.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration;

NodesConfiguration buildNodesConfiguration(NodeID my_id, int num_shards) {
  NodesConfigurationTestUtil::NodeTemplate tmpl{my_id.index()};
  tmpl.num_shards = num_shards;
  tmpl.generation = my_id.generation();

  NodesConfiguration nc;
  auto new_nc =
      nc.applyUpdate(NodesConfigurationTestUtil::addNewNodeUpdate(nc, tmpl));
  ld_check(new_nc);

  // Assert that all the shards are PROVISIONING
  for (shard_index_t shard = 0; shard < num_shards; shard++) {
    ld_check(new_nc->getStorageMembership()
                 ->getShardState(ShardID(my_id.index(), shard))
                 ->storage_state == membership::StorageState::PROVISIONING);
  }
  return *new_nc;
}

TEST(RebuildingMarkerCheckerTest, testAllProvisioning) {
  node_index_t node_idx = 0;
  node_gen_t generation = 2;
  int num_shards = 5;
  auto store = std::make_unique<ShardedTemporaryLogStore>(num_shards);
  auto nc = buildNodesConfiguration(NodeID(node_idx, generation), num_shards);
  auto nc_api = std::make_unique<MockNodesConfigurationManager>(nc);

  RebuildingMarkerChecker checker(
      nc.getStorageMembership()->getShardStates(node_idx),
      NodeID(node_idx, generation),
      nc_api.get(),
      store.get());

  auto res = checker.checkAndWriteMarkers();
  ASSERT_TRUE(res.hasValue());

  // All shards are provisioning, so they all should be marked as not missing
  // data even if they don't have the marker.
  EXPECT_EQ(
      (std::unordered_map<shard_index_t, RebuildingMarkerChecker::CheckResult>{
          {0, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {1, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {2, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {3, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {4, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
      }),
      res.value());

  // All the shards should be moved to NONE & that the marker is written
  auto new_nc = nc_api->getConfig();
  for (shard_index_t shard = 0; shard < num_shards; shard++) {
    EXPECT_EQ(membership::StorageState::NONE,
              new_nc->getStorageMembership()
                  ->getShardState(ShardID(node_idx, shard))
                  ->storage_state);

    RebuildingCompleteMetadata meta;
    int rv = store->getByIndex(shard)->readStoreMetadata(&meta);
    EXPECT_EQ(0, rv);
  }
}

TEST(RebuildingMarkerCheckerTest, testFirstGeneration) {
  node_index_t node_idx = 0;
  node_gen_t generation = 1;
  int num_shards = 5;
  auto store = std::make_unique<ShardedTemporaryLogStore>(num_shards);
  auto nc = buildNodesConfiguration(NodeID(node_idx, generation), num_shards);
  nc = *nc.applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(nc));

  RebuildingMarkerChecker checker(
      nc.getStorageMembership()->getShardStates(node_idx),
      NodeID(node_idx, generation),
      nullptr /* simulating a disabled NCM */,
      store.get());

  auto res = checker.checkAndWriteMarkers();
  ASSERT_TRUE(res.hasValue());

  // The node's generation is 1, so we assume that they don't have the
  // rebuilding marker.
  EXPECT_EQ(
      (std::unordered_map<shard_index_t, RebuildingMarkerChecker::CheckResult>{
          {0, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {1, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {2, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {3, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {4, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
      }),
      res.value());

  // Expect that the rebuilding marker got written
  for (shard_index_t shard = 0; shard < num_shards; shard++) {
    RebuildingCompleteMetadata meta;
    int rv = store->getByIndex(shard)->readStoreMetadata(&meta);
    EXPECT_EQ(0, rv);
  }
}

TEST(RebuildingMarkerCheckerTest, testMissingData) {
  node_index_t node_idx = 0;
  node_gen_t generation = 2;
  int num_shards = 5;
  auto store = std::make_unique<ShardedTemporaryLogStore>(num_shards);
  auto nc = buildNodesConfiguration(NodeID(node_idx, generation), num_shards);
  nc = *nc.applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(nc));
  auto nc_api = std::make_unique<MockNodesConfigurationManager>(nc);

  {
    // Let's write the marker for shards 1, 2 and 4.
    RebuildingCompleteMetadata metadata;
    LocalLogStore::WriteOptions options;
    int rv = store->getByIndex(1)->writeStoreMetadata(metadata, options);
    ASSERT_EQ(0, rv);
    rv = store->getByIndex(2)->writeStoreMetadata(metadata, options);
    ASSERT_EQ(0, rv);
    rv = store->getByIndex(4)->writeStoreMetadata(metadata, options);
    ASSERT_EQ(0, rv);
  }

  RebuildingMarkerChecker checker(
      nc.getStorageMembership()->getShardStates(node_idx),
      NodeID(node_idx, generation),
      nc_api.get(),
      store.get());

  auto res = checker.checkAndWriteMarkers();
  ASSERT_TRUE(res.hasValue());

  // The node's generation is 2, and neither of the shards are provisioning
  // so the ones that don't have the marker will be reported as missing data.
  EXPECT_EQ(
      (std::unordered_map<shard_index_t, RebuildingMarkerChecker::CheckResult>{
          {0, RebuildingMarkerChecker::CheckResult::SHARD_MISSING_DATA},
          {1, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {2, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
          {3, RebuildingMarkerChecker::CheckResult::SHARD_MISSING_DATA},
          {4, RebuildingMarkerChecker::CheckResult::SHARD_NOT_MISSING_DATA},
      }),
      res.value());
}
}} // namespace facebook::logdevice
