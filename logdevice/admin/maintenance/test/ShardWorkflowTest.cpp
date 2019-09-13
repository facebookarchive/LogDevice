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
  bool nc_stuck_{false};
};

class MockShardWorkflow : public ShardWorkflow {
 public:
  explicit MockShardWorkflow(ShardWorkflowTest* test, ShardID shard)
      : ShardWorkflow(shard, nullptr), test_(test) {}

  void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<void(Status st, lsn_t lsn, const std::string& str)> cb)
      const override;

  bool isNcTransitionStuck() const override {
    return test_->nc_stuck_;
  }

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
  membership::ShardState shard_state;
  shard_state.storage_state = membership::StorageState::READ_WRITE;
  auto result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DISABLING_WRITE);

  shard_state.storage_state = membership::StorageState::READ_ONLY;
  result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_START_DATA_MIGRATION);
  ASSERT_NE(event, nullptr);

  SHARD_NEEDS_REBUILD_flags_t expected_flag{SHARD_NEEDS_REBUILD_Header::DRAIN};
  ASSERT_EQ(
      expected_flag,
      (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->header.flags);

  EventType expected_event_type{EventType::SHARD_NEEDS_REBUILD};
  ASSERT_EQ(expected_event_type,
            (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->getType());
  event = nullptr;

  result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::START_DATA_MIGRATION);
  ASSERT_EQ(event, nullptr);

  shard_state.storage_state = membership::StorageState::DATA_MIGRATION;
  result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::RELOCATE);
  ASSERT_EQ(
      std::move(result).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);
  ASSERT_EQ(event, nullptr);

  folly::SemiFuture<MaintenanceStatus> f3 =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(f3).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);
  folly::SemiFuture<MaintenanceStatus> f4 =
      wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RELOCATE);
  ASSERT_EQ(
      std::move(f4).get(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DATA_MIGRATION_COMPLETED);

  shard_state.storage_state = membership::StorageState::NONE;
  folly::SemiFuture<MaintenanceStatus> f5 =
      wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(f5).get(), MaintenanceStatus::COMPLETED);
}

TEST_F(ShardWorkflowTest, SimpleMayDisappear) {
  init();
  wf->addTargetOpState({ShardOperationalState::MAY_DISAPPEAR});
  wf->shouldSkipSafetyCheck(false);
  membership::ShardState shard_state;
  shard_state.storage_state = membership::StorageState::READ_WRITE;
  auto f =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DISABLING_WRITE);

  shard_state.storage_state = membership::StorageState::READ_ONLY;
  f = wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
  ASSERT_EQ(event, nullptr);
}

TEST_F(ShardWorkflowTest, SimpleEnable) {
  init();
  wf->addTargetOpState({ShardOperationalState::ENABLED});
  membership::ShardState shard_state;
  shard_state.storage_state = membership::StorageState::PROVISIONING;
  auto f =
      wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODE_PROVISIONING);

  shard_state.storage_state = membership::StorageState::NONE;
  f = wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLING_READ);

  shard_state.storage_state = membership::StorageState::READ_ONLY;
  f = wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLE_WRITE);

  shard_state.storage_state = membership::StorageState::READ_WRITE;
  f = wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
}

TEST_F(ShardWorkflowTest, SimpleEnableWithMiniRebuilding) {
  init();
  wf->addTargetOpState({ShardOperationalState::ENABLED});
  membership::ShardState shard_state;
  shard_state.storage_state = membership::StorageState::READ_ONLY;
  auto f = wf->run(
      shard_state, ShardDataHealth::LOST_REGIONS, RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(event, nullptr);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::ENABLE_WRITE);

  shard_state.storage_state = membership::StorageState::READ_WRITE;
  f = wf->run(
      shard_state, ShardDataHealth::LOST_REGIONS, RebuildingMode::RESTORE);
  ASSERT_TRUE(f.isReady());
  ASSERT_EQ(f.value(), MaintenanceStatus::COMPLETED);
}

TEST_F(ShardWorkflowTest, NCStuckInTransitionalState) {
  init();
  wf->addTargetOpState({ShardOperationalState::DRAINED});
  wf->shouldSkipSafetyCheck(true);
  wf->rebuildInRestoreMode(true);
  membership::ShardState shard_state;
  shard_state.storage_state = membership::StorageState::READ_WRITE;
  auto result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DISABLING_WRITE);

  shard_state.storage_state = membership::StorageState::RW_TO_RO;
  result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);

  // NC is not considered stuck yet. No rebuilding event should be created
  ASSERT_EQ(event, nullptr);

  // Say it has been stuck for a while
  nc_stuck_ = true;
  result =
      wf->run(shard_state, ShardDataHealth::HEALTHY, RebuildingMode::INVALID);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);

  // We should have kicked off rebuilding
  ASSERT_NE(event, nullptr);
  SHARD_NEEDS_REBUILD_flags_t non_expected_flag{
      SHARD_NEEDS_REBUILD_Header::DRAIN};
  ASSERT_NE(
      non_expected_flag,
      (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->header.flags);

  EventType expected_event_type{EventType::SHARD_NEEDS_REBUILD};
  ASSERT_EQ(expected_event_type,
            (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->getType());
  event = nullptr;

  shard_state.storage_state = membership::StorageState::READ_ONLY;
  // Now NC update goes through. We should write a new event to event log
  result = wf->run(
      shard_state, ShardDataHealth::UNAVAILABLE, RebuildingMode::RESTORE);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_START_DATA_MIGRATION);
  // We should have kicked off rebuilding
  ASSERT_NE(event, nullptr);
  ASSERT_NE(
      non_expected_flag,
      (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->header.flags);
  ASSERT_EQ(expected_event_type,
            (static_cast<SHARD_NEEDS_REBUILD_Event*>(event.get()))->getType());
  event = nullptr;

  // Say event log write goes through
  result = wf->run(
      shard_state, ShardDataHealth::UNAVAILABLE, RebuildingMode::RESTORE);
  ASSERT_EQ(std::move(result).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::START_DATA_MIGRATION);

  shard_state.storage_state = membership::StorageState::DATA_MIGRATION;
  result = wf->run(
      shard_state, ShardDataHealth::UNAVAILABLE, RebuildingMode::RESTORE);
  ASSERT_EQ(
      std::move(result).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);
  // No new event will be created, since we are already rebuilding
  ASSERT_EQ(event, nullptr);

  folly::SemiFuture<MaintenanceStatus> f3 = wf->run(
      shard_state, ShardDataHealth::UNAVAILABLE, RebuildingMode::RESTORE);
  ASSERT_EQ(std::move(f3).get(), MaintenanceStatus::AWAITING_DATA_REBUILDING);
  folly::SemiFuture<MaintenanceStatus> f4 =
      wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RESTORE);
  ASSERT_EQ(
      std::move(f4).get(), MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  ASSERT_EQ(wf->getExpectedStorageStateTransition(),
            membership::StorageStateTransition::DATA_MIGRATION_COMPLETED);
  shard_state.storage_state = membership::StorageState::NONE;
  folly::SemiFuture<MaintenanceStatus> f5 =
      wf->run(shard_state, ShardDataHealth::EMPTY, RebuildingMode::RELOCATE);
  ASSERT_EQ(std::move(f5).get(), MaintenanceStatus::COMPLETED);
}

}}} // namespace facebook::logdevice::maintenance
