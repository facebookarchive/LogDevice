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

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

TEST(SequencerWorkflowTest, EnableSequencer) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::ENABLED);
  auto future = wf->run(false /*Sequencer currently disabled*/);
  ASSERT_EQ(std::move(future).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  future = wf->run(true /*Sequencer now enabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result
  future = wf->run(true /*Sequencer now enabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}

TEST(SequencerWorkflowTest, DisableSequencer) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::DISABLED);
  auto future = wf->run(true /*Sequencer currently enabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::AWAITING_SAFETY_CHECK);
  future = wf->run(false /*Sequencer now disabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result
  future = wf->run(false /*Sequencer now disabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}

TEST(SequencerWorkflowTest, DisableSequencerSkipSafety) {
  auto wf = std::make_unique<SequencerWorkflow>(node_index_t(1));
  wf->setTargetOpState(SequencingState::DISABLED);
  wf->shouldSkipSafetyCheck(true);
  auto future = wf->run(true /*Sequencer currently enabled*/);
  ASSERT_EQ(std::move(future).get(),
            MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
  future = wf->run(false /*Sequencer now disabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
  // Calling run again when target state has already been reached
  // should return same result
  future = wf->run(false /*Sequencer now disabled*/);
  ASSERT_EQ(std::move(future).get(), MaintenanceStatus::COMPLETED);
}
