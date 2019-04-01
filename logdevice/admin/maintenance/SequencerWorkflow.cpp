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
  return std::move(promise_future.second);
}

void SequencerWorkflow::setTargetOpState(SequencingState state) {
  target_op_state_ = state;
}

void SequencerWorkflow::shouldSkipSafetyCheck(bool skip) {
  skip_safety_check_ = skip;
}

}}} // namespace facebook::logdevice::maintenance
