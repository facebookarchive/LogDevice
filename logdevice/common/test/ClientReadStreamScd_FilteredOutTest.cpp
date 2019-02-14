/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <numeric>

#include <gtest/gtest.h>

#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::NodeSetTestUtil;

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)

class ClientReadStreamScd_FilteredOutTest : public ::testing::Test {
 public:
  using FilteredOut = ClientReadStreamScd::FilteredOut;
  void SetUp() override {
    const size_t nodeset_size{5};
    const copyset_size_t replication{4};
    const logid_t LOG_ID{1};

    configuration::Nodes nodes;
    // for simplicity just use 5 nodes with 1 shard each
    addNodes(&nodes, nodeset_size, 1, "rg0.dc0.cl0.ro0.rk1", 5);
    storage_set_ = StorageSet{N0, N1, N2, N3, N4};

    Configuration::NodesConfig nodes_config;
    nodes_config.setNodes(std::move(nodes));

    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    addLog(logs_config.get(), LOG_ID, replication, 0, nodeset_size, {});

    config_ = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(
            "failure_domain_test", std::move(nodes_config)),
        std::move(logs_config));
  }

  void assert_equal_shardset(small_shardset_t left, small_shardset_t right) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
    ASSERT_EQ(left, right);
  }
  void
  assert_shards(small_shardset_t expected_new_down,
                small_shardset_t expected_new_slow,
                small_shardset_t expected_current_down = small_shardset_t{},
                small_shardset_t expected_current_slow = small_shardset_t{}) {
    small_shardset_t actual_down;
    small_shardset_t actual_slow;

    for (const ShardID& shard : filtered_out.getNewShardsSlow()) {
      actual_slow.push_back(shard);
    }
    for (const ShardID& shard : filtered_out.getNewShardsDown()) {
      actual_down.push_back(shard);
    }
    assert_equal_shardset(expected_new_down, actual_down);
    assert_equal_shardset(expected_new_slow, actual_slow);
    assert_equal_shardset(expected_current_down, filtered_out.getShardsDown());
    assert_equal_shardset(expected_current_slow, filtered_out.getShardsSlow());
  }
  ~ClientReadStreamScd_FilteredOutTest() override {}

 protected:
  FilteredOut filtered_out;

 private:
  std::shared_ptr<Configuration> config_;
  StorageSet storage_set_;
};

// Testing basic functionality of deferredChangeShardsSlow(),
// deferredAddShardDown(), deferredRemoveShardDown(
TEST_F(ClientReadStreamScd_FilteredOutTest, DeferredAddShardBasic) {
  // Add it to the shards slow first..
  assert_shards({}, {});
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N0}));
  assert_shards({}, {N0});
  // Scheduling the same change should fail.
  ASSERT_FALSE(filtered_out.deferredChangeShardsSlow({N0}));
  assert_shards({}, {N0});
  // Clearing the shards slow list
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({}));
  assert_shards({}, {});
  // re-removing it from the shards slow..
  ASSERT_FALSE(filtered_out.deferredChangeShardsSlow({}));
  assert_shards({}, {});

  // Testing basic functionality of deferredAddShardDown
  // Add it to the shards down first..
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  assert_shards({N0}, {});
  // Re-adding it to the shards down should fail
  ASSERT_FALSE(filtered_out.deferredAddShardDown(N0));
  assert_shards({N0}, {});
  // Remove it from the shards down.
  ASSERT_TRUE(filtered_out.deferredRemoveShardDown(N0));
  assert_shards({}, {});
  ASSERT_FALSE(filtered_out.deferredRemoveShardDown(N0));
  assert_shards({}, {});
}

// Verify that when a slow shard is added to the shards down list, it will be
// immediately kicked out from the slow shards list.
// Also verify that a down shard can't be added to the slow shards list
TEST_F(ClientReadStreamScd_FilteredOutTest,
       DeferredAddShardWhenAlreadyHasOtherTag) {
  assert_shards({}, {});
  // add N0 to the slow shards list
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N0}));
  assert_shards({}, {N0});
  // then add it to the down shards list
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  // check it's been kicked out from the slow shards list
  assert_shards({N0}, {});
}

TEST_F(ClientReadStreamScd_FilteredOutTest, ApplyDeferredChangesBasic) {
  assert_shards({}, {}, {}, {});
  // no changes
  ASSERT_FALSE(filtered_out.applyDeferredChanges());
  assert_shards({}, {}, {}, {});

  // Adding new shards down/slow
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N1}));
  assert_shards({N0}, {N1}, {}, {});
  ASSERT_TRUE(filtered_out.applyDeferredChanges());
  assert_shards({N0}, {N1}, {N0}, {N1});
  assert_equal_shardset(small_shardset_t{N0, N1}, filtered_out.getAllShards());

  // Removing existing shards down/slow
  ASSERT_TRUE(filtered_out.deferredRemoveShardDown(N0));
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({}));
  assert_shards({}, {}, {N0}, {N1});
  ASSERT_TRUE(filtered_out.applyDeferredChanges());
  assert_shards({}, {}, {}, {});
}

TEST_F(ClientReadStreamScd_FilteredOutTest, Clear) {
  assert_shards({}, {}, {}, {});
  // clear when is clear already, nothing happens
  filtered_out.clear();
  assert_shards({}, {}, {}, {});

  // Add some shards to the filtered out list
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N1}));
  assert_shards({N0}, {N1}, {}, {});
  ASSERT_TRUE(filtered_out.applyDeferredChanges());
  assert_shards({N0}, {N1}, {N0}, {N1});
  assert_equal_shardset(small_shardset_t{N0, N1}, filtered_out.getAllShards());

  filtered_out.clear();
  assert_shards({}, {}, {}, {});
  assert_equal_shardset(small_shardset_t{}, filtered_out.getAllShards());
}

TEST_F(ClientReadStreamScd_FilteredOutTest, EraseShard) {
  assert_shards({}, {}, {}, {});
  // clear when is clear already, nothing happens
  filtered_out.clear();
  assert_shards({}, {}, {}, {});

  // Add some shards to the filtered out list
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N1}));
  assert_shards({N0}, {N1}, {}, {});
  ASSERT_TRUE(filtered_out.applyDeferredChanges());
  assert_shards({N0}, {N1}, {N0}, {N1});
  assert_equal_shardset(small_shardset_t{N0, N1}, filtered_out.getAllShards());

  // Erase a down shard and verify it is erased both from current and new down
  // shards lists
  ASSERT_TRUE(filtered_out.eraseShard(N0));
  assert_shards({}, {N1}, {}, {N1});
  assert_equal_shardset(small_shardset_t{N1}, filtered_out.getAllShards());

  // Erase a slow shard and verify it is erased both from current and new slow
  // shards lists
  ASSERT_TRUE(filtered_out.eraseShard(N1));
  assert_shards({}, {}, {}, {});
  assert_equal_shardset(small_shardset_t{}, filtered_out.getAllShards());

  // Verify eraseShard() returns true only if the shard was in a current list
  ASSERT_TRUE(filtered_out.deferredAddShardDown(N0));
  ASSERT_TRUE(filtered_out.deferredChangeShardsSlow({N1}));
  assert_shards({N0}, {N1}, {}, {});
  // N1 will be removed from the deffered slow list, but eraseShard() will
  // return false since N1 was now in the current slow shards list
  ASSERT_FALSE(filtered_out.eraseShard(N1));
  assert_shards({N0}, {}, {}, {});
  // same for N0
  ASSERT_FALSE(filtered_out.eraseShard(N0));
  assert_shards({}, {}, {}, {});
}
}} // namespace facebook::logdevice
