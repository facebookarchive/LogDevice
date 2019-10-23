/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/CheckpointStateMachine.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using checkpointing::thrift::CheckpointDelta;
using checkpointing::thrift::CheckpointState;

CheckpointStateMachine::CheckpointStateMachine(logid_t delta_log_id,
                                               logid_t snapshot_log_id)
    : ReplicatedStateMachine<CheckpointState, CheckpointDelta>(
          RSMType::CHECKPOINT_STATE_MACHINE,
          delta_log_id,
          snapshot_log_id) {}

std::unique_ptr<CheckpointState>
CheckpointStateMachine::makeDefaultState(lsn_t) const {
  // TODO: Not implemented.
  return nullptr;
}

std::unique_ptr<CheckpointState>
CheckpointStateMachine::deserializeState(Payload payload,
                                         lsn_t version,
                                         std::chrono::milliseconds) const {
  auto state = ThriftCodec::deserialize<BinarySerializer, CheckpointState>(
      Slice(payload));
  if (!state) {
    ld_warning("Failed to deserialize CheckpointState with version: %s",
               toString(version).c_str());
    return nullptr;
  }
  return state;
}

std::unique_ptr<CheckpointDelta>
CheckpointStateMachine::deserializeDelta(Payload payload) {
  auto delta = ThriftCodec::deserialize<BinarySerializer, CheckpointDelta>(
      Slice(payload));
  if (!delta) {
    ld_warning("Failed to deserialize the payload from checkpoint delta");
    return nullptr;
  }
  return delta;
}

int CheckpointStateMachine::applyDelta(const CheckpointDelta& delta,
                                       CheckpointState& state,
                                       lsn_t version,
                                       std::chrono::milliseconds,
                                       std::string& failure_reason) {
  auto type = delta.getType();
  switch (type) {
    case CheckpointDelta::Type::update_checkpoint: {
      state.checkpoints[delta.get_update_checkpoint().customer_id] =
          delta.get_update_checkpoint().checkpoint;
      state.set_version(version);
      return 0;
    }
    case CheckpointDelta::Type::remove_checkpoint: {
      // TODO: Not implemented
      return 0;
    }
    default:
      ld_warning("Unknown type of CheckpointDelta. Not applying the delta");
      failure_reason = "Unknown type";
      return -1;
  }
}

int CheckpointStateMachine::serializeState(const CheckpointState& state,
                                           void* buf,
                                           size_t buf_size) {
  auto serialized_state =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(state);
  if (buf != nullptr) {
    ld_check(buf_size >= serialized_state.size());
    memcpy(buf, serialized_state.data(), serialized_state.size());
  }
  return serialized_state.size();
}

bool CheckpointStateMachine::shouldCreateSnapshot() const {
  // TODO: Not implemented
  return false;
}
bool CheckpointStateMachine::canSnapshot() const {
  // TODO: Not implemented
  return false;
}
void CheckpointStateMachine::onSnapshotCreated(Status, size_t){
    // TODO: Not implemented
};
}} // namespace facebook::logdevice
