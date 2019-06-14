/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/SequencerWorkflow.h"

namespace facebook { namespace logdevice { namespace maintenance {

folly::SemiFuture<MaintenanceStatus>
SequencerWorkflow::run(bool is_sequencer_enabled) {
  current_sequencing_state_ = is_sequencer_enabled ? SequencingState::ENABLED
                                                   : SequencingState::DISABLED;

  auto promise_future = folly::makePromiseContract<MaintenanceStatus>();

  if (target_op_state_ == current_sequencing_state_) {
    promise_future.first.setValue(MaintenanceStatus::COMPLETED);
  } else {
    if (target_op_state_ == SequencingState::ENABLED || skip_safety_check_) {
      promise_future.first.setValue(
          MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
    } else {
      ld_check(target_op_state_ == SequencingState::DISABLED);
      promise_future.first.setValue(MaintenanceStatus::AWAITING_SAFETY_CHECK);
    }
  }
  // This gets updated by the last time we evaluated this function. Even if the
  // status didn't change we know that we evaluated it recently.
  last_updated_at_ = SystemTimestamp::now();
  return std::move(promise_future.second);
}

void SequencerWorkflow::setTargetOpState(SequencingState state) {
  target_op_state_ = state;
}

void SequencerWorkflow::shouldSkipSafetyCheck(bool skip) {
  skip_safety_check_ = skip;
}

SequencingState SequencerWorkflow::getTargetOpState() const {
  return target_op_state_;
}

SystemTimestamp SequencerWorkflow::getLastUpdatedTimestamp() const {
  return last_updated_at_;
}

SystemTimestamp SequencerWorkflow::getCreationTimestamp() const {
  return created_at_;
}

}}} // namespace facebook::logdevice::maintenance
