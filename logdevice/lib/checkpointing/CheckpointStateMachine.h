/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"
#include "logdevice/lib/checkpointing/if/gen-cpp2/Checkpoint_types.h"

namespace facebook { namespace logdevice {

/**
 * CheckpointStateMachine is a replicated state machine that maintains the
 * checkpoint state in memory. It's used by checkpoint store to get and update
 * checkpoints. The structure of the checkpoint state and check point delta is
 * documented in Checkpoint.thrift file. For detailed information on how this
 * works, check out the documentation on ReplicatatedStateMachine.h
 */
class CheckpointStateMachine
    : public facebook::logdevice::ReplicatedStateMachine<
          checkpointing::thrift::CheckpointState,
          checkpointing::thrift::CheckpointDelta> {
 public:
  explicit CheckpointStateMachine(logid_t delta_log_id,
                                  logid_t snapshot_log_id = LOGID_INVALID);

  std::unique_ptr<checkpointing::thrift::CheckpointState>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<checkpointing::thrift::CheckpointState>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

  std::unique_ptr<checkpointing::thrift::CheckpointDelta>
  deserializeDelta(Payload payload) override;

  int applyDelta(const checkpointing::thrift::CheckpointDelta& delta,
                 checkpointing::thrift::CheckpointState& state,
                 lsn_t version,
                 std::chrono::milliseconds timestamp,
                 std::string& failure_reason) override;

  int serializeState(const checkpointing::thrift::CheckpointState& state,
                     void* buf,
                     size_t buf_size) override;

  bool shouldCreateSnapshot() const override;
  bool canSnapshot() const override;
  void onSnapshotCreated(Status st, size_t snapshotSize) override;
};
}} // namespace facebook::logdevice
