/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/membership/StorageMembership.h"

#include <gtest/gtest.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/MembershipThriftConverter.h"
#include "logdevice/common/membership/StorageMembership.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::membership::MembershipVersion;
using namespace facebook::logdevice::membership::MaintenanceID;

namespace {

#define N0 ShardID(0, 1)
#define N1 ShardID(1, 1)
#define N2 ShardID(2, 1)
#define N3 ShardID(3, 1)
#define N4 ShardID(4, 1)
#define N5 ShardID(5, 1)
#define N6 ShardID(6, 1)
#define N7 ShardID(7, 1)
#define N8 ShardID(8, 1)
#define N9 ShardID(9, 1)
#define N10 ShardID(10, 1)

constexpr MaintenanceID::Type DUMMY_MAINTENANCE{2333};

using StateOverride = ShardState::Update::StateOverride;

class StorageMembershipTest : public ::testing::Test {
 public:
  static StorageMembership::Update genUpdateOneShard(
      ShardID shard,
      uint64_t base_ver,
      StorageStateTransition transition,
      StateTransitionCondition conditions,
      folly::Optional<StateOverride> state_override = folly::none) {
    StorageMembership::Update res{MembershipVersion::Type(base_ver)};
    MaintenanceID::Type maintenance =
        (isProvisionShard(transition) ? MAINTENANCE_PROVISION
                                      : DUMMY_MAINTENANCE);
    int rv = res.addShard(
        shard, {transition, conditions, maintenance, state_override});
    EXPECT_EQ(0, rv);
    return res;
  }

  // add a set of shards of the same transition into an existing update
  static void
  addShards(StorageMembership::Update* update,
            const std::set<ShardID>& shards,
            StorageStateTransition transition,
            StateTransitionCondition conditions,
            folly::Optional<StateOverride> state_override = folly::none) {
    ld_check(update != nullptr);
    MaintenanceID::Type maintenance =
        (isProvisionShard(transition) ? MAINTENANCE_PROVISION
                                      : DUMMY_MAINTENANCE);
    for (auto shard : shards) {
      int rv = update->addShard(
          shard, {transition, conditions, maintenance, state_override});
      EXPECT_EQ(0, rv);
    }
  }

  static StorageMembership::Update
  genUpdateShards(const std::set<ShardID>& shards,
                  uint64_t base_ver,
                  StorageStateTransition transition,
                  StateTransitionCondition conditions,
                  folly::Optional<StateOverride> state_override = folly::none) {
    StorageMembership::Update res{MembershipVersion::Type(base_ver)};
    addShards(&res, shards, transition, conditions, state_override);
    return res;
  }

  inline void checkCodecSerialization(const StorageMembership& m) {
    auto got = MembershipThriftConverter::fromThrift(
        MembershipThriftConverter::toThrift(m));

    ASSERT_NE(nullptr, got);
    ASSERT_EQ(m, *got);
  }
};

#define ASSERT_SHARD_STATE_FULL(_m,                            \
                                _shard,                        \
                                _storage_state,                \
                                _metadata_state,               \
                                _flags,                        \
                                _since_version,                \
                                _maintenance)                  \
  do {                                                         \
    auto res = _m.getShardState(_shard);                       \
    EXPECT_TRUE(res.first);                                    \
    EXPECT_EQ(_storage_state, res.second.storage_state);       \
    EXPECT_EQ(_flags, res.second.flags);                       \
    EXPECT_EQ(_metadata_state, res.second.metadata_state);     \
    EXPECT_EQ(_maintenance, res.second.active_maintenance);    \
    EXPECT_EQ(_since_version, res.second.since_version.val()); \
  } while (0)

#define ASSERT_SHARD_STATE(                                              \
    _m, _shard, _storage_state, _metadata_state, _flags, _since_version) \
  ASSERT_SHARD_STATE_FULL(_m,                                            \
                          _shard,                                        \
                          _storage_state,                                \
                          _metadata_state,                               \
                          _flags,                                        \
                          _since_version,                                \
                          DUMMY_MAINTENANCE)

#define ASSERT_NO_SHARD(_m, _shard)      \
  do {                                   \
    auto res = _m.getShardState(_shard); \
    EXPECT_FALSE(res.first);             \
  } while (0)

#define ASSERT_MEMBERSHIP_NODES(_m, ...)                      \
  do {                                                        \
    auto nodes = _m.getMembershipNodes();                     \
    auto expected = std::vector<node_index_t>({__VA_ARGS__}); \
    std::sort(nodes.begin(), nodes.end());                    \
    std::sort(expected.begin(), expected.end());              \
    EXPECT_EQ(expected, nodes);                               \
  } while (0)

#define CHECK_METADATA_SHARDS(_m, ...)                                         \
  do {                                                                         \
    auto metadata_set = StorageSet({__VA_ARGS__});                             \
    std::sort(metadata_set.begin(), metadata_set.end(), std::less<ShardID>()); \
    EXPECT_EQ(_m.getMetaDataStorageSet(), metadata_set);                       \
    EXPECT_EQ(std::set<ShardID>({__VA_ARGS__}), _m.getMetaDataShards());       \
    for (auto shard : metadata_set) {                                          \
      auto res = _m.getShardState(shard);                                      \
      EXPECT_TRUE(res.first);                                                  \
      EXPECT_TRUE(res.second.isValid());                                       \
      EXPECT_TRUE(                                                             \
          res.second.metadata_state == MetaDataStorageState::METADATA ||       \
          res.second.metadata_state == MetaDataStorageState::PROMOTING);       \
    }                                                                          \
    EXPECT_TRUE(_m.validate());                                                \
  } while (0)

TEST_F(StorageMembershipTest, EmptyShardStateInvalid) {
  ASSERT_FALSE(ShardState().isValid());
  ASSERT_EQ(EMPTY_VERSION, ShardState().since_version);
}

TEST_F(StorageMembershipTest, EmptyStorageMembershipValid) {
  ASSERT_TRUE(StorageMembership().validate());
  ASSERT_EQ(EMPTY_VERSION, StorageMembership().getVersion());
  ASSERT_EQ(0, StorageMembership().numNodes());
  ASSERT_TRUE(StorageMembership().isEmpty());
  ASSERT_MEMBERSHIP_NODES(StorageMembership());
}

// go through the life cycle of a shard with all successful transitions
TEST_F(StorageMembershipTest, ShardLifeCycle) {
  StorageMembership m{};
  // add one empty shard N1
  int rv =
      m.applyUpdate(genUpdateOneShard(N1,
                                      EMPTY_VERSION.val(),
                                      StorageStateTransition::ADD_EMPTY_SHARD,
                                      Condition::NONE),
                    &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  // version should have bumped to 1
  ASSERT_EQ(1, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);

  // add another empty shard N2, N1's state should stay intact
  rv = m.applyUpdate(
      genUpdateOneShard(
          N2, 1, StorageStateTransition::ADD_EMPTY_SHARD, Condition::NONE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(2, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);
  ASSERT_SHARD_STATE(m,
                     N2,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     2);

  // enabling read
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       2,
                                       StorageStateTransition::ENABLING_READ,
                                       (Condition::EMPTY_SHARD |
                                        Condition::LOCAL_STORE_READABLE |
                                        Condition::NO_SELF_REPORT_MISSING_DATA |
                                        Condition::CAUGHT_UP_LOCAL_CONFIG)),
                     &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(3, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     3);

  // commit read enabled
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        3,
                        StorageStateTransition::COMMIT_READ_ENABLED,
                        Condition::COPYSET_CONFIRMATION),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(4, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_ONLY,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     4);

  // enable write
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       4,
                                       StorageStateTransition::ENABLE_WRITE,
                                       Condition::LOCAL_STORE_WRITABLE),
                     &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(5, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     5);

  // disable write
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       5,
                                       StorageStateTransition::DISABLING_WRITE,
                                       (Condition::WRITE_AVAILABILITY_CHECK |
                                        Condition::CAPACITY_CHECK)),
                     &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(6, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::RW_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     6);

  // mark the shard N1 as UNRECOVERABLE
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        6,
                        StorageStateTransition::MARK_SHARD_UNRECOVERABLE,
                        (Condition::SELF_AWARE_MISSING_DATA |
                         Condition::CANNOT_ACCEPT_WRITES)),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(7, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::RW_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::UNRECOVERABLE,
                     7);

  // commit write disabled
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        7,
                        StorageStateTransition::COMMIT_WRITE_DISABLED,
                        Condition::FMAJORITY_CONFIRMATION),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(8, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_ONLY,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::UNRECOVERABLE,
                     8);

  // start data migration
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        8,
                        StorageStateTransition::START_DATA_MIGRATION,
                        Condition::CAPACITY_CHECK),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(9, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::DATA_MIGRATION,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::UNRECOVERABLE,
                     9);

  // data migration completed
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        9,
                        StorageStateTransition::DATA_MIGRATION_COMPLETED,
                        Condition::DATA_MIGRATION_COMPLETE),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(10, m.getVersion().val());
  // UNRECOVERABLE flags are removed
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     10);

  // remove empty shard N1
  rv = m.applyUpdate(
      genUpdateOneShard(
          N1, 10, StorageStateTransition::REMOVE_EMPTY_SHARD, Condition::NONE),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(11, m.getVersion().val());
  ASSERT_NO_SHARD(m, N1);
  // N2 should stay intact
  ASSERT_EQ(1, m.numNodes());
  ASSERT_SHARD_STATE(m,
                     N2,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     2);
  ASSERT_MEMBERSHIP_NODES(m, 2);
  checkCodecSerialization(m);
}

// test various invalid transitions
TEST_F(StorageMembershipTest, InvalidTransitions) {
  StorageMembership m{};
  int rv;
  // add one empty shard N1
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       EMPTY_VERSION.val(),
                                       StorageStateTransition::ADD_EMPTY_SHARD,
                                       Condition::NONE),
                     &m);
  ASSERT_EQ(0, rv);

  // remove one shard that doesn't exist
  rv = m.applyUpdate(
      genUpdateOneShard(
          N2, 1, StorageStateTransition::REMOVE_EMPTY_SHARD, Condition::NONE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOTINCONFIG, err);

  // add a shard that already exists
  rv = m.applyUpdate(
      genUpdateOneShard(
          N1, 1, StorageStateTransition::ADD_EMPTY_SHARD, Condition::FORCE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::EXISTS, err);

  auto enable_read = genUpdateOneShard(
      N1,
      1,
      StorageStateTransition::ENABLING_READ,
      (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
       Condition::NO_SELF_REPORT_MISSING_DATA |
       Condition::CAUGHT_UP_LOCAL_CONFIG));

  // try to apply an invalid update
  auto update_invalid = enable_read;
  update_invalid.shard_updates.clear();
  ASSERT_FALSE(update_invalid.isValid());
  rv = m.applyUpdate(update_invalid, &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);

  // try to apply an update with wrong base version
  auto wrong_base = enable_read;
  wrong_base.base_version = MembershipVersion::Type{2};
  rv = m.applyUpdate(wrong_base, &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);

  // try to apply an update with insufficient conditions
  auto wrong_conditions = enable_read;
  // remove one condition
  wrong_conditions.shard_updates[N1].conditions &=
      (~Condition::NO_SELF_REPORT_MISSING_DATA);
  rv = m.applyUpdate(wrong_conditions, &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::CONDITION_MISMATCH, err);

  // try to apply an update which is not compatible with current
  // shard state
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       1,
                                       StorageStateTransition::ENABLE_WRITE,
                                       Condition::LOCAL_STORE_WRITABLE),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SOURCE_STATE_MISMATCH, err);
  checkCodecSerialization(m);
}

// test that the force flag can override condition checks
TEST_F(StorageMembershipTest, ForceFlag) {
  StorageMembership m{};
  int rv;
  // add one empty shard N1
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       EMPTY_VERSION.val(),
                                       StorageStateTransition::ADD_EMPTY_SHARD,
                                       Condition::NONE),
                     &m);
  ASSERT_EQ(0, rv);
  // enabling read will fail with insufficient conditions given
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       1,
                                       StorageStateTransition::ENABLING_READ,
                                       (Condition::EMPTY_SHARD |
                                        Condition::LOCAL_STORE_READABLE |
                                        Condition::CAUGHT_UP_LOCAL_CONFIG)),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::CONDITION_MISMATCH, err);

  // However, adding the FORCE flag will make the transition bypass condition
  // checks
  rv = m.applyUpdate(
      genUpdateOneShard(
          N1, 1, StorageStateTransition::ENABLING_READ, Condition::FORCE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     2);
  checkCodecSerialization(m);
}

TEST_F(StorageMembershipTest, StateOverride) {
  StorageMembership m{};
  int rv;
  // add one empty shard N1
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       EMPTY_VERSION.val(),
                                       StorageStateTransition::ADD_EMPTY_SHARD,
                                       Condition::NONE),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);

  // invalid update, state_override not provided
  rv = m.applyUpdate(
      genUpdateOneShard(
          N1, 1, StorageStateTransition::OVERRIDE_STATE, Condition::FORCE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);

  // not using the FORCE condition
  StateOverride st_override{StorageState::READ_WRITE,
                            StorageStateFlags::UNRECOVERABLE,
                            MetaDataStorageState::METADATA};
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       1,
                                       StorageStateTransition::OVERRIDE_STATE,
                                       Condition::EMPTY_SHARD,
                                       {st_override}),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::CONDITION_MISMATCH, err);

  // apply the override
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       1,
                                       StorageStateTransition::OVERRIDE_STATE,
                                       Condition::FORCE,
                                       {st_override}),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::UNRECOVERABLE,
                     2);
  checkCodecSerialization(m);
}

// test the behavior of marking shard as unrecoverable
TEST_F(StorageMembershipTest, UNRECOVERABLE) {
  StorageMembership m{};
  int rv;
  // add one empty shard N1
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       EMPTY_VERSION.val(),
                                       StorageStateTransition::ADD_EMPTY_SHARD,
                                       Condition::NONE),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);

  // marking shard as unrecoverable in NONE will be a no-op
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        1,
                        StorageStateTransition::MARK_SHARD_UNRECOVERABLE,
                        (Condition::SELF_AWARE_MISSING_DATA |
                         Condition::CANNOT_ACCEPT_WRITES)),
      &m);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     2);

  // enabling read
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       2,
                                       StorageStateTransition::ENABLING_READ,
                                       (Condition::EMPTY_SHARD |
                                        Condition::LOCAL_STORE_READABLE |
                                        Condition::NO_SELF_REPORT_MISSING_DATA |
                                        Condition::CAUGHT_UP_LOCAL_CONFIG)),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     3);

  // marking the shard as unrecoverable will transition it back to NONE
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        3,
                        StorageStateTransition::MARK_SHARD_UNRECOVERABLE,
                        (Condition::SELF_AWARE_MISSING_DATA |
                         Condition::CANNOT_ACCEPT_WRITES)),
      &m);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     4);
  checkCodecSerialization(m);
}

// test that behavior of manipulating metadata storage shards
TEST_F(StorageMembershipTest, MetaDataShards) {
  StorageMembership m{};
  int rv;
  // add two regualr shards N1, N3, and one metadata shard N2
  auto update =
      genUpdateOneShard(N2,
                        EMPTY_VERSION.val(),
                        StorageStateTransition::ADD_EMPTY_METADATA_SHARD,
                        Condition::NONE);
  addShards(&update,
            {N1, N3},
            StorageStateTransition::ADD_EMPTY_SHARD,
            Condition::NONE);
  rv = m.applyUpdate(update, &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(3, m.numNodes());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);
  ASSERT_SHARD_STATE(m,
                     N2,
                     StorageState::NONE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     1);
  ASSERT_SHARD_STATE(m,
                     N3,
                     StorageState::NONE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     1);
  CHECK_METADATA_SHARDS(m, N2);

  // transition N1 and N2 to RW, and N3 to RO
  rv = m.applyUpdate(genUpdateShards({N1, N2, N3},
                                     MIN_VERSION.val(),
                                     StorageStateTransition::ENABLING_READ,
                                     (Condition::EMPTY_SHARD |
                                      Condition::LOCAL_STORE_READABLE |
                                      Condition::NO_SELF_REPORT_MISSING_DATA |
                                      Condition::CAUGHT_UP_LOCAL_CONFIG)),
                     &m);

  ASSERT_EQ(0, rv);
  rv =
      m.applyUpdate(genUpdateShards({N1, N2, N3},
                                    2,
                                    StorageStateTransition::COMMIT_READ_ENABLED,
                                    Condition::COPYSET_CONFIRMATION),
                    &m);

  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(genUpdateShards({N1, N2},
                                     3,
                                     StorageStateTransition::ENABLE_WRITE,
                                     Condition::LOCAL_STORE_WRITABLE),
                     &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(4, m.getVersion().val());
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     4);
  ASSERT_SHARD_STATE(m,
                     N2,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     4);
  ASSERT_SHARD_STATE(m,
                     N3,
                     StorageState::READ_ONLY,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     3);
  CHECK_METADATA_SHARDS(m, N2);

  // try promoting N2 to a metadata shard would fail as N2 is already a
  // metadata shard
  rv = m.applyUpdate(
      genUpdateOneShard(N2,
                        4,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SOURCE_STATE_MISMATCH, err);

  // try promoting N3 to become a metadata shard would fail as N3 is not in RW
  rv = m.applyUpdate(
      genUpdateOneShard(N3,
                        4,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::SOURCE_STATE_MISMATCH, err);
  CHECK_METADATA_SHARDS(m, N2);

  // promoting N1 to a metadata shard
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        4,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::PROMOTING,
                     StorageStateFlags::NONE,
                     5);
  CHECK_METADATA_SHARDS(m, N1, N2);

  // commit the promotion
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        5,
                        StorageStateTransition::COMMIT_PROMOTION_METADATA_SHARD,
                        Condition::COPYSET_CONFIRMATION),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     6);
  CHECK_METADATA_SHARDS(m, N1, N2);
  // now enable writes on N3 and promoting it to be metadata shard
  rv = m.applyUpdate(genUpdateShards({N3},
                                     6,
                                     StorageStateTransition::ENABLE_WRITE,
                                     Condition::LOCAL_STORE_WRITABLE),
                     &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N3,
                        7,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N3,
                     StorageState::READ_WRITE,
                     MetaDataStorageState::PROMOTING,
                     StorageStateFlags::NONE,
                     8);
  CHECK_METADATA_SHARDS(m, N1, N2, N3);

  // disabling writes for N3, it will automatically cancel the promotion
  rv = m.applyUpdate(genUpdateOneShard(N3,
                                       8,
                                       StorageStateTransition::DISABLING_WRITE,
                                       (Condition::WRITE_AVAILABILITY_CHECK |
                                        Condition::CAPACITY_CHECK)),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N3,
                     StorageState::RW_TO_RO,
                     MetaDataStorageState::NONE,
                     StorageStateFlags::NONE,
                     9);
  CHECK_METADATA_SHARDS(m, N1, N2);

  // disabling writes for N1, as N1 is a metadata shard, it requires additional
  // conditions
  rv = m.applyUpdate(genUpdateOneShard(N1,
                                       9,
                                       StorageStateTransition::DISABLING_WRITE,
                                       (Condition::WRITE_AVAILABILITY_CHECK |
                                        Condition::CAPACITY_CHECK)),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::CONDITION_MISMATCH, err);
  CHECK_METADATA_SHARDS(m, N1, N2);

  // add the required conditions and it should succeed
  rv = m.applyUpdate(
      genUpdateOneShard(
          N1,
          9,
          StorageStateTransition::DISABLING_WRITE,
          (Condition::WRITE_AVAILABILITY_CHECK | Condition::CAPACITY_CHECK |
           Condition::METADATA_WRITE_AVAILABILITY_CHECK |
           Condition::METADATA_CAPACITY_CHECK)),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::RW_TO_RO,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     10);
  CHECK_METADATA_SHARDS(m, N1, N2);

  // perform data migration and transition N1 to be NONE, it should preserve
  // its metadata status
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        10,
                        StorageStateTransition::COMMIT_WRITE_DISABLED,
                        (Condition::FMAJORITY_CONFIRMATION)),
      &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        11,
                        StorageStateTransition::START_DATA_MIGRATION,
                        Condition::CAPACITY_CHECK),
      &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        12,
                        StorageStateTransition::DATA_MIGRATION_COMPLETED,
                        Condition::DATA_MIGRATION_COMPLETE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N1,
                     StorageState::NONE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     13);
  ASSERT_MEMBERSHIP_NODES(m, 1, 2, 3);
  checkCodecSerialization(m);
}

TEST_F(StorageMembershipTest, InvalidProvisionUpdate) {
  // invalid provision update with wrong maintenance ID
  ShardState::Update invalid1{StorageStateTransition::PROVISION_SHARD,
                              Condition::NONE,
                              MAINTENANCE_NONE};

  ASSERT_FALSE(invalid1.isValid());
  ShardState::Update invalid2{StorageStateTransition::PROVISION_METADATA_SHARD,
                              Condition::NONE,
                              MaintenanceID::Type(50)};
  ASSERT_FALSE(invalid2.isValid());

  StorageMembership m{};
  // add one shard so that membership is not empty anymore
  int rv =
      m.applyUpdate(genUpdateOneShard(N1,
                                      EMPTY_VERSION.val(),
                                      StorageStateTransition::ADD_EMPTY_SHARD,
                                      Condition::NONE),
                    &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  ASSERT_EQ(1, m.getVersion().val());

  // further provision transitions should fail because of the
  // base version check

  // add another empty shard N2, N1's state should stay intact
  rv = m.applyUpdate(
      genUpdateOneShard(
          N2, 1, StorageStateTransition::PROVISION_SHARD, Condition::FORCE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);

  rv = m.applyUpdate(
      genUpdateOneShard(N2,
                        1,
                        StorageStateTransition::PROVISION_METADATA_SHARD,
                        Condition::FORCE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);
  checkCodecSerialization(m);
}

TEST_F(StorageMembershipTest, ProvisionTransition) {
  StorageMembership m{};
  int rv;
  // provision m with three regular shard N1, N2, N4 and one metadata shard N3
  auto update = genUpdateShards(
      {N1, N2, N4},
      EMPTY_VERSION.val(),
      StorageStateTransition::PROVISION_SHARD,
      (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
       Condition::NO_SELF_REPORT_MISSING_DATA |
       Condition::LOCAL_STORE_WRITABLE));

  addShards(&update,
            {N3},
            StorageStateTransition::PROVISION_METADATA_SHARD,
            (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
             Condition::NO_SELF_REPORT_MISSING_DATA |
             Condition::LOCAL_STORE_WRITABLE));
  rv = m.applyUpdate(update, &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(4, m.numNodes());
  ASSERT_SHARD_STATE_FULL(m,
                          N1,
                          StorageState::READ_WRITE,
                          MetaDataStorageState::NONE,
                          StorageStateFlags::NONE,
                          1,
                          MAINTENANCE_PROVISION);
  ASSERT_SHARD_STATE_FULL(m,
                          N2,
                          StorageState::READ_WRITE,
                          MetaDataStorageState::NONE,
                          StorageStateFlags::NONE,
                          1,
                          MAINTENANCE_PROVISION);
  ASSERT_SHARD_STATE_FULL(m,
                          N3,
                          StorageState::READ_WRITE,
                          MetaDataStorageState::METADATA,
                          StorageStateFlags::NONE,
                          1,
                          MAINTENANCE_PROVISION);
  ASSERT_SHARD_STATE_FULL(m,
                          N4,
                          StorageState::READ_WRITE,
                          MetaDataStorageState::NONE,
                          StorageStateFlags::NONE,
                          1,
                          MAINTENANCE_PROVISION);
  CHECK_METADATA_SHARDS(m, N3);

  // further provision should fail
  rv = m.applyUpdate(
      genUpdateOneShard(N7,
                        1,
                        StorageStateTransition::PROVISION_METADATA_SHARD,
                        Condition::FORCE),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);

  // but can still add shard
  rv = m.applyUpdate(
      genUpdateOneShard(N7,
                        1,
                        StorageStateTransition::ADD_EMPTY_METADATA_SHARD,
                        Condition::FORCE),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_SHARD_STATE(m,
                     N7,
                     StorageState::NONE,
                     MetaDataStorageState::METADATA,
                     StorageStateFlags::NONE,
                     2);
  CHECK_METADATA_SHARDS(m, N3, N7);
  ASSERT_MEMBERSHIP_NODES(m, 1, 2, 3, 4, 7);
  checkCodecSerialization(m);
}

///////////  Testing the flatbuffers Codec //////////////////

TEST_F(StorageMembershipTest, CodecEmptyMembership) {
  // serialize and deserialize an empty membership
  StorageMembership m{};
  checkCodecSerialization(m);
}

TEST_F(StorageMembershipTest, CodecBasic) {
  StorageMembership m{};
  int rv;
  auto update = genUpdateShards({N5, N6, N7},
                                EMPTY_VERSION.val(),
                                StorageStateTransition::PROVISION_SHARD,
                                Condition::FORCE);
  addShards(&update,
            {N8, N9},
            StorageStateTransition::PROVISION_METADATA_SHARD,
            Condition::FORCE);
  rv = m.applyUpdate(update, &m);
  ASSERT_EQ(0, rv);
  auto update2 = genUpdateOneShard(
      N2, 1, StorageStateTransition::ADD_EMPTY_METADATA_SHARD, Condition::NONE);
  addShards(&update2,
            {N1, N3},
            StorageStateTransition::ADD_EMPTY_SHARD,
            Condition::NONE);
  rv = m.applyUpdate(update2, &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(genUpdateShards({N1, N2, N3},
                                     2,
                                     StorageStateTransition::ENABLING_READ,
                                     Condition::FORCE),
                     &m);
  ASSERT_EQ(0, rv);
  rv =
      m.applyUpdate(genUpdateShards({N1, N2, N3},
                                    3,
                                    StorageStateTransition::COMMIT_READ_ENABLED,
                                    Condition::COPYSET_CONFIRMATION),
                    &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(genUpdateShards({N1, N2},
                                     4,
                                     StorageStateTransition::ENABLE_WRITE,
                                     Condition::LOCAL_STORE_WRITABLE),
                     &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        5,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N1,
                        6,
                        StorageStateTransition::COMMIT_PROMOTION_METADATA_SHARD,
                        Condition::COPYSET_CONFIRMATION),
      &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(genUpdateShards({N3},
                                     7,
                                     StorageStateTransition::ENABLE_WRITE,
                                     Condition::LOCAL_STORE_WRITABLE),
                     &m);
  ASSERT_EQ(0, rv);
  rv = m.applyUpdate(
      genUpdateOneShard(N3,
                        8,
                        StorageStateTransition::PROMOTING_METADATA_SHARD,
                        Condition::NONE),
      &m);
  ASSERT_EQ(0, rv);
  CHECK_METADATA_SHARDS(m, N1, N2, N3, N8, N9);
  checkCodecSerialization(m);
}

// TODO: test getting writer's and reader's view?

} // namespace
