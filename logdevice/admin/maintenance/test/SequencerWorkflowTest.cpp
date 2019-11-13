/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/SequencerWorkflow.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

TEST(SequencerWorkflowTest, EnableSequencer) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::ENABLED);
  membership::SequencerNodeState node_state;
  node_state.sequencer_enabled = false;
  // We cannot allow the ENABLE workflow if the node is dead.
  auto future = wf->run(node_state, ClusterStateNodeState::DEAD);
  ASSERT_EQ(
      std::move(future).get(), MaintenanceStatus::AWAITING_NODE_TO_BE_ALIVE);

  // STARTING should be sufficient to move forward.
  future = wf->run(node_state, ClusterStateNodeState::STARTING);
  ASSERT_EQ(std::move(future).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  node_state.sequencer_enabled = true;
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result. Even if the node is dead.
  future = wf->run(node_state, ClusterStateNodeState::DEAD);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}

TEST(SequencerWorkflowTest, ManualOverrideBlocksEnableSequencer) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::ENABLED);
  membership::SequencerNodeState node_state;
  node_state.sequencer_enabled = false;
  node_state.manual_override = true;
  auto future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(
      std::move(future).get(), MaintenanceStatus::BLOCKED_BY_ADMIN_OVERRIDE);

  node_state.manual_override = false;
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);

  node_state.sequencer_enabled = true;
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}

TEST(SequencerWorkflowTest, DisableSequencer) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::DISABLED);
  membership::SequencerNodeState node_state;
  node_state.sequencer_enabled = true;
  auto future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  node_state.sequencer_enabled = false;
  // Being dead should have no effect.
  future = wf->run(node_state, ClusterStateNodeState::DEAD);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}

TEST(SequencerWorkflowTest, DisableSequencerSkipSafety) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::DISABLED);
  wf->shouldSkipSafetyCheck(true);
  membership::SequencerNodeState node_state;
  node_state.sequencer_enabled = true;
  auto future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  node_state.sequencer_enabled = false;
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result
  future = wf->run(node_state, ClusterStateNodeState::FULLY_STARTED);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}
