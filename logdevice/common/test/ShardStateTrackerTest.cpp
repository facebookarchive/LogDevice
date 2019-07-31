/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/ShardStateTracker.h"

#include <folly/portability/GTest.h>

#include "logdevice/common/membership/utils.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::NodesConfigurationTestUtil;

TEST(ShardStateTrackerTest, empty) {
  ShardStateTracker t{};
  {
    auto update_opt = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_FALSE(update_opt.hasValue());
  }

  {
    auto nc = std::make_shared<const NodesConfiguration>();
    t.onNewConfig(nc);

    auto update_opt = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_FALSE(update_opt.hasValue());
  }
}

namespace {
// verify that update takes every shard out of an intermediary state
void verify_update(std::shared_ptr<const NodesConfiguration> base_config,
                   folly::Optional<NodesConfiguration::Update> update_opt) {
  EXPECT_TRUE(update_opt.hasValue());

  auto new_config = base_config->applyUpdate(std::move(update_opt).value());
  EXPECT_NE(nullptr, new_config);
  EXPECT_TRUE(new_config->validate());

  const auto& sm = new_config->getStorageMembership();
  auto nodes = sm->getMembershipNodes();
  for (auto n : nodes) {
    auto shard_states = sm->getShardStates(n);
    for (auto& sp : shard_states) {
      auto& shard_state = sp.second;
      EXPECT_FALSE(isIntermediaryState(shard_state.storage_state))
          << toString(shard_state.storage_state);
      EXPECT_FALSE(isIntermediaryState(shard_state.metadata_state))
          << toString(shard_state.metadata_state);
      EXPECT_LE(shard_state.since_version,
                new_config->getStorageMembership()->getVersion());
    }
  }
}
} // namespace

TEST(ShardStateTrackerTest, basic) {
  ShardStateTracker t{};
  auto nc = provisionNodes();
  ASSERT_TRUE(nc->validate());
  t.onNewConfig(nc);

  SystemTimestamp ts1 = SystemTimestamp::now();
  // NodesConfiguration::applyUpdate takes the current timestamp as the
  // last_change_timestamp. sleeping here is a lazy way to similate a config at
  // a higher time without bumping its version.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  auto nc2 = nc->applyUpdate(addNewNodeUpdate(*nc));
  ASSERT_TRUE(nc2->validate());
  auto ts2 = nc2->getLastChangeTimestamp();
  t.onNewConfig(nc2);
  {
    // nc2 has no shards in intermediary states
    auto update_opt = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_FALSE(update_opt.hasValue());
  }

  nc2 = nc2->applyUpdate(markAllShardProvisionedUpdate(*nc2));
  ASSERT_TRUE(nc2->validate());
  ts2 = nc2->getLastChangeTimestamp();
  t.onNewConfig(nc2);
  {
    // nc2 has no shards in intermediary states
    auto update_opt = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_FALSE(update_opt.hasValue());
  }

  auto nc3 = nc2->applyUpdate(enablingReadUpdate(nc2->getVersion()));
  ASSERT_TRUE(nc3->validate());
  auto ts3 = nc3->getLastChangeTimestamp();
  ASSERT_GE(ts3, ts2);
  t.onNewConfig(nc3);

  auto verify_n17_enabling_read = [](std::shared_ptr<const NodesConfiguration>
                                         base_config,
                                     folly::Optional<NodesConfiguration::Update>
                                         update_opt) {
    EXPECT_TRUE(update_opt.hasValue());
    auto& sm_update = update_opt->storage_config_update->membership_update;
    EXPECT_EQ(base_config->getStorageMembership()->getVersion(),
              sm_update->base_version);
    auto it = sm_update->shard_updates.find(ShardID{17, 0});
    EXPECT_TRUE(it != sm_update->shard_updates.end());

    auto new_config = base_config->applyUpdate(std::move(update_opt).value());
    EXPECT_NE(nullptr, new_config);
    EXPECT_TRUE(new_config->validate());
    auto p = new_config->getStorageMembership()->getShardState(ShardID{17, 0});
    EXPECT_TRUE(p.hasValue());
    EXPECT_EQ(StorageState::READ_ONLY, p->storage_state);
    EXPECT_EQ(MetaDataStorageState::NONE, p->metadata_state);
    EXPECT_EQ(
        new_config->getStorageMembership()->getVersion(), p->since_version);
  };

  {
    auto update_opt = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_TRUE(update_opt.hasValue());
    auto update_opt2 = t.extractNCUpdate(ts3);
    EXPECT_TRUE(update_opt2.hasValue());

    EXPECT_TRUE(update_opt->isValid());
    EXPECT_EQ(
        update_opt->storage_config_update->membership_update->shard_updates,
        update_opt2->storage_config_update->membership_update->shard_updates);

    verify_n17_enabling_read(nc3, std::move(update_opt));
    verify_update(nc3, std::move(update_opt2));
  }
  {
    // using an old ts will return no shards in intermediary states
    auto update_opt = t.extractNCUpdate(ts1);
    EXPECT_FALSE(update_opt.hasValue());
  }

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  auto nc4 = nc3->applyUpdate(disablingWriteUpdate(nc3->getVersion()));
  // The NC version advances but the StorageMembership version does not.
  nc4 = nc4->withIncrementedVersionAndTimestamp();
  nc4 = nc4->withIncrementedVersionAndTimestamp();
  auto ts3_2 = SystemTimestamp::now();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  // advance StorageMembership version (as well as NC version and timestamp)
  nc4 = nc4->withIncrementedVersionAndTimestamp(
      /* new_nc_version = */ membership::MembershipVersion::Type{1234},
      /* new_sequencer_membership_version = */ folly::none,
      /* new_storage_membership_version = */
      membership::MembershipVersion::Type{1234});
  auto ts4 = nc4->getLastChangeTimestamp();

  ASSERT_TRUE(nc4->validate());
  ASSERT_GT(ts4, ts2);
  t.onNewConfig(nc4);
  {
    // N17 stays in the same intermediary state, other shards enter
    // intermediary states. check that ts of N17 does not change.
    auto update_opt = t.extractNCUpdate(ts3);
    EXPECT_TRUE(update_opt.hasValue());
    verify_n17_enabling_read(nc4, std::move(update_opt));

    update_opt = t.extractNCUpdate(ts3_2);
    EXPECT_TRUE(update_opt.hasValue());
    verify_n17_enabling_read(nc4, std::move(update_opt));

    auto update_opt2 = t.extractNCUpdate(ts4);
    EXPECT_TRUE(update_opt2.hasValue());
    verify_update(nc4, std::move(update_opt2));
  }

  SystemTimestamp ts5 = SystemTimestamp::now();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  std::shared_ptr<const NodesConfiguration> nc7;

  {
    // skip versions of nc, N17 no longer in intermediary state; N11 is in a
    // different intermediary state; N13 is removed.
    NodesConfiguration::Update update{};
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            nc4->getStorageMembership()->getVersion());

    update.storage_config_update->membership_update->addShard(
        ShardID{17, 0},
        {StorageStateTransition::OVERRIDE_STATE,
         Condition::FORCE,
         DUMMY_MAINTENANCE,
         ShardState::Update::StateOverride{StorageState::READ_WRITE,
                                           /* flags = */ {},
                                           MetaDataStorageState::METADATA}});
    update.storage_config_update->membership_update->addShard(
        ShardID{11, 0},
        {StorageStateTransition::OVERRIDE_STATE,
         Condition::FORCE,
         DUMMY_MAINTENANCE,
         ShardState::Update::StateOverride{StorageState::READ_WRITE,
                                           /* flags = */ {},
                                           MetaDataStorageState::PROMOTING}});
    update.storage_config_update->membership_update->addShard(
        ShardID{13, 0},
        {StorageStateTransition::OVERRIDE_STATE,
         Condition::FORCE,
         DUMMY_MAINTENANCE,
         ShardState::Update::StateOverride{StorageState::NONE,
                                           /* flags = */ {},
                                           MetaDataStorageState::NONE}});
    auto nc5 = nc4->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, nc5);
    EXPECT_TRUE(nc5->validate());

    NodesConfiguration::Update update2{};
    update2.storage_config_update = std::make_unique<StorageConfig::Update>();
    update2.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            nc5->getStorageMembership()->getVersion());

    update2.storage_config_update->membership_update->addShard(
        ShardID{13, 0},
        {StorageStateTransition::REMOVE_EMPTY_SHARD,
         Condition::NONE,
         DUMMY_MAINTENANCE,
         /* state_override = */ folly::none});

    auto nc6 = nc5->applyUpdate(std::move(update2));
    EXPECT_NE(nullptr, nc6);
    EXPECT_TRUE(nc6->validate());

    NodesConfiguration::Update update3{};
    update3.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update3.service_discovery_update->addNode(
        node_index_t{13},
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});

    nc7 = nc6->applyUpdate(std::move(update3));
    EXPECT_NE(nullptr, nc7);

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    nc7 = nc7->withIncrementedVersionAndTimestamp();
    nc7 = nc7->withIncrementedVersionAndTimestamp();
    nc7 = nc7->withIncrementedVersionAndTimestamp();
    EXPECT_NE(nullptr, nc7);
    EXPECT_TRUE(nc7->validate());

    t.onNewConfig(nc7);
  }
  {
    // N17 is no longer in intermediary state, even if we query with an earlier
    // timestamp
    auto update_opt = t.extractNCUpdate(ts5);
    EXPECT_FALSE(update_opt.hasValue());
  }
  SystemTimestamp ts7 = nc7->getLastChangeTimestamp();
  {
    auto update_opt = t.extractNCUpdate(ts7);
    auto update_opt2 = t.extractNCUpdate(SystemTimestamp::now());
    EXPECT_TRUE(update_opt.hasValue());
    EXPECT_TRUE(update_opt->isValid());
    EXPECT_EQ(
        update_opt->storage_config_update->membership_update->shard_updates,
        update_opt2->storage_config_update->membership_update->shard_updates);

    verify_update(nc7, std::move(update_opt));
    auto new_config = nc7->applyUpdate(std::move(update_opt2).value());
    EXPECT_NE(nullptr, new_config);
    EXPECT_TRUE(new_config->validate());

    auto& sm = new_config->getStorageMembership();
    EXPECT_FALSE(sm->getShardState(ShardID{13, 0}).hasValue());
    EXPECT_EQ(StorageState::READ_WRITE,
              sm->getShardState(ShardID{17, 0})->storage_state);
    EXPECT_EQ(MetaDataStorageState::METADATA,
              sm->getShardState(ShardID{17, 0})->metadata_state);
    EXPECT_EQ(toNonIntermediaryState(MetaDataStorageState::PROMOTING),
              sm->getShardState(ShardID{11, 0})->metadata_state);
  }

  std::shared_ptr<const NodesConfiguration> nc8;
  SystemTimestamp ts8;
  {
    // N11 transition into the same intermediary state but has a different
    // since_version
    NodesConfiguration::Update update{};
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            nc7->getStorageMembership()->getVersion());
    update.storage_config_update->membership_update->addShard(
        ShardID{11, 0},
        {StorageStateTransition::OVERRIDE_STATE,
         Condition::FORCE,
         DUMMY_MAINTENANCE,
         ShardState::Update::StateOverride{StorageState::READ_WRITE,
                                           /* flags = */ {},
                                           MetaDataStorageState::PROMOTING}});

    nc8 = nc7->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, nc7);

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    nc8 = nc8->withIncrementedVersionAndTimestamp();
    ts8 = nc8->getLastChangeTimestamp();
    t.onNewConfig(nc8);
  }
  {
    auto update_opt = t.extractNCUpdate(ts7);
    EXPECT_FALSE(update_opt.hasValue());

    update_opt = t.extractNCUpdate(ts8);
    EXPECT_TRUE(update_opt.hasValue());
    EXPECT_TRUE(update_opt->isValid());
    auto& shard_updates =
        update_opt->storage_config_update->membership_update->shard_updates;
    EXPECT_NE(shard_updates.end(), shard_updates.find(ShardID{11, 0}));
    verify_update(nc8, std::move(update_opt));
  }

  {
    // The tracker should ignore old nc versions
    t.onNewConfig(nc3);
    auto update_opt = t.extractNCUpdate(ts7);
    EXPECT_FALSE(update_opt.hasValue());
  }
}
