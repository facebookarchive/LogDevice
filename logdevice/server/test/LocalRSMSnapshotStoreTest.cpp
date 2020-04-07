/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/server/LocalRSMSnapshotStoreImpl.h"

namespace facebook { namespace logdevice {
class LocalRSMSnapshotStoreTest : public ::testing::Test {
 public:
  LocalRSMSnapshotStoreTest() {}

  void init(logid_t delta_log) {
    snapshot_store_ = std::make_unique<LocalRSMSnapshotStoreImpl>(
        folly::to<std::string>(delta_log.val_), true /* writable */);
  }

  std::unique_ptr<LocalRSMSnapshotStoreImpl> snapshot_store_;
};

TEST_F(LocalRSMSnapshotStoreTest, GetDurableReplicatinProperty_SingleScope) {
  init(logid_t(1));
  size_t clsize = 10;

  ReplicationProperty rp_in, res;
  for (int r = 1; r <= 2; r++) {
    rp_in.setReplication(NodeLocationScope::NODE, r);
    res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
    ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), r * 2);
  }

  for (int r = 3; r <= 5; r++) {
    rp_in.setReplication(NodeLocationScope::NODE, r);
    res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
    ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), 5);
  }

  for (int r = 6; r <= 10; r++) {
    rp_in.setReplication(NodeLocationScope::NODE, r);
    res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
    ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), r);
  }
}

TEST_F(LocalRSMSnapshotStoreTest, GetDurableReplicatinProperty_VanillaConfig) {
  init(logid_t(1));
  size_t clsize = 24;

  ReplicationProperty rp_in, res;
  rp_in.setReplication(NodeLocationScope::CLUSTER, 3);
  rp_in.setReplication(NodeLocationScope::NODE, 6);
  res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
  ASSERT_EQ(res.getReplication(NodeLocationScope::CLUSTER), 3);
  ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), 12);
}

TEST_F(LocalRSMSnapshotStoreTest, GetDurableReplicatinProperty_RackScope) {
  init(logid_t(1));
  size_t clsize = 24;

  ReplicationProperty rp_in, res;
  rp_in.setReplication(NodeLocationScope::RACK, 3);
  rp_in.setReplication(NodeLocationScope::NODE, 6);
  res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
  ASSERT_EQ(res.getReplication(NodeLocationScope::RACK), 3);
  ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), 12);

  rp_in.setReplication(NodeLocationScope::RACK, 4);
  rp_in.setReplication(NodeLocationScope::NODE, 8);
  res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
  ASSERT_EQ(res.getReplication(NodeLocationScope::RACK), 4);
  ASSERT_EQ(res.getReplication(NodeLocationScope::NODE), 12);
}

TEST_F(LocalRSMSnapshotStoreTest,
       GetDurableReplicatinProperty_NoImplicitNodeScope) {
  init(logid_t(1));
  size_t clsize = 24;

  ReplicationProperty res, expected_res;
  ReplicationProperty rp_in({{NodeLocationScope::REGION, 3}});
  expected_res = ReplicationProperty(
      {{NodeLocationScope::REGION, 3}, {NodeLocationScope::NODE, 6}});
  res = snapshot_store_->getDurableReplicationProperty(rp_in, clsize);
  ASSERT_EQ(res, expected_res);

  ReplicationProperty rp_in2({{NodeLocationScope::RACK, 3}});
  expected_res = ReplicationProperty(
      {{NodeLocationScope::RACK, 3}, {NodeLocationScope::NODE, 6}});
  res = snapshot_store_->getDurableReplicationProperty(rp_in2, clsize);
  ASSERT_EQ(res, expected_res);

  // Multiple Scopes
  ReplicationProperty rp_in3(
      {{NodeLocationScope::REGION, 3}, {NodeLocationScope::RACK, 6}});
  expected_res = ReplicationProperty({{NodeLocationScope::REGION, 3},
                                      {NodeLocationScope::RACK, 6},
                                      {NodeLocationScope::NODE, 12}});
  res = snapshot_store_->getDurableReplicationProperty(rp_in3, clsize);
  ASSERT_EQ(res, expected_res);
}

}} // namespace facebook::logdevice
