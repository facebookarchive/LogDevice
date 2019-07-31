/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/ShardWorkflow.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#define SHARD ShardID(1, 1)

namespace facebook { namespace logdevice { namespace maintenance {

class MockShardWorkflow;
class ShardWorkflowTest : public ::testing::Test {
 public:
  void init();
  std::unique_ptr<MockShardWorkflow> wf;
  std::unique_ptr<EventLogRecord> event;
  std::function<void(Status st, lsn_t lsn, const std::string& str)> sink;
};

class MockShardWorkflow : public ShardWorkflow {
 public:
  explicit MockShardWorkflow(ShardWorkflowTest* test, ShardID shard)
      : ShardWorkflow(shard, nullptr), test_(test) {}

  void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<void(Status st, lsn_t lsn, const std::string& str)> cb)
      const override;

 private:
  ShardWorkflowTest* test_;
};

void MockShardWorkflow::writeToEventLog(
    std::unique_ptr<EventLogRecord> event,
    std::function<void(Status st, lsn_t lsn, const std::string& str)> cb)
    const {
  test_->event = std::move(event);
  cb(E::OK, lsn_t(1), "Dummy");
}

void ShardWorkflowTest::init() {
  wf = std::make_unique<MockShardWorkflow>(this, SHARD);
}

TEST_F(ShardWorkflowTest, SimpleDrain) {
  init();
  wf->addTargetOpState({ShardOperationalState::DRAINED});
  wf->shouldSkipSafetyCheck(false);
  auto result = wf->run(membership::StorageState::READ_WRITE,
                        ShardDataHealth::HEALTHY,
                        RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DISABLING_WRITE);

  result = wf->run(membership::StorageState::READ_ONLY,
                   ShardDataHealth::HEALTHY,
                   RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::START_DATA_MIGRATION);
  result = wf->run(membership::StorageState::DATA_MIGRATION,
                   ShardDataHealth::HEALTHY,
                   RebuildingMode::INVALID);
  ASSERT_EQ(
      std::move(result).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);

  ASSERT_NE(event, nullptr);

  SHARD_NEEDS_REBUILD_flags_t expected_flag{SHARD_NEEDS_REBUILD_Header::DRAIN};
  ASSERT_EQ(
      expected_flag,
      (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->header.flags);

  EventType expected_event_type{EventType::SHARD_NEEDS_REBUILD};
  ASSERT_EQ(expected_event_type,
            (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->getType());

  folly::SemiFuture<MaintenanceStatus> f3 =
      wf->run(membership::StorageState::DATA_MIGRATION,
              ShardDataHealth::HEALTHY,
              RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(f3).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);
  folly::SemiFuture<MaintenanceStatus> f4 =
      wf->run(membership::StorageState::DATA_MIGRATION,
              ShardDataHealth::EMPTY,
              RebuildingMode::RELOCATE);
  ASSERT_EQ(
      std::move(f4).get(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DATA_MIGRATION_COMPLETED);
  folly::SemiFuture<MaintenanceStatus> f5 =
      wf->run(membership::StorageState::NONE,
              ShardDataHealth::EMPTY,
              RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(f5).get(), MaintenanceStatus::COMPLETED);
}

TEST_F(ShardWorkflowTest, SimpleMayDisappear) {
  init();
  wf->addTargetOpState({ShardOperationalState::MAY_DISAPPEAR});
  wf->shouldSkipSafetyCheck(false);
  auto f = wf->run(membership::StorageState::READ_WRITE,
                   ShardDataHealth::HEALTHY,
                   RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DISABLING_WRITE);
  f = wf->run(membership::StorageState::READ_ONLY,
              ShardDataHealth::HEALTHY,
              RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
  ASSERT_EQ(event, nullptr);
}

TEST_F(ShardWorkflowTest, SimpleEnable) {
  init();
  wf->addTargetOpState({ShardOperationalState::ENABLED});
  auto f = wf->run(membership::StorageState::PROVISIONING,
                   ShardDataHealth::EMPTY,
                   RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODE_PROVISIONING);
  f = wf->run(membership::StorageState::NONE,
              ShardDataHealth::EMPTY,
              RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLING_READ);
  f = wf->run(membership::StorageState::READ_ONLY,
              ShardDataHealth::HEALTHY,
              RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLE_WRITE);
  f = wf->run(membership::StorageState::READ_WRITE,
              ShardDataHealth::HEALTHY,
              RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
}

TEST_F(ShardWorkflowTest, SimpleEnableWithMiniRebuilding) {
  init();
  wf->addTargetOpState({ShardOperationalState::ENABLED});
  auto f = wf->run(membership::StorageState::READ_ONLY,
                   ShardDataHealth::LOST_REGIONS,
                   RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(event, nullptr);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLE_WRITE);
  f = wf->run(membership::StorageState::READ_WRITE,
              ShardDataHealth::LOST_REGIONS,
              RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
}

}}} // namespace facebook::logdevice::maintenance
