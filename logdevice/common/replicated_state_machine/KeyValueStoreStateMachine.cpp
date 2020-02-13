/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/replicated_state_machine/KeyValueStoreStateMachine.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"

namespace facebook { namespace logdevice {

using apache::thrift::BinarySerializer;
using replicated_state_machine::thrift::KeyValueStoreDelta;
using replicated_state_machine::thrift::KeyValueStoreState;

KeyValueStoreStateMachine::KeyValueStoreStateMachine(logid_t delta_log_id,
                                                     logid_t snapshot_log_id)
    : ReplicatedStateMachine<KeyValueStoreState, KeyValueStoreDelta>(
          RSMType::KEY_VALUE_STORE_STATE_MACHINE,
          delta_log_id,
          snapshot_log_id) {}

std::unique_ptr<KeyValueStoreState>
KeyValueStoreStateMachine::makeDefaultState(lsn_t version) const {
  auto state = std::make_unique<KeyValueStoreState>();
  state->version = version;
  return state;
}

std::unique_ptr<KeyValueStoreState>
KeyValueStoreStateMachine::deserializeState(Payload payload,
                                            lsn_t version,
                                            std::chrono::milliseconds) const {
  auto state = ThriftCodec::deserialize<BinarySerializer, KeyValueStoreState>(
      Slice(payload));
  if (!state) {
    ld_warning("Failed to deserialize KeyValueStoreState with version: %s",
               toString(version).c_str());
    return nullptr;
  }
  return state;
}

std::unique_ptr<KeyValueStoreDelta>
KeyValueStoreStateMachine::deserializeDelta(Payload payload) {
  auto delta = ThriftCodec::deserialize<BinarySerializer, KeyValueStoreDelta>(
      Slice(payload));
  if (!delta) {
    ld_warning("Failed to deserialize the payload from KeyValueStore delta");
    return nullptr;
  }
  return delta;
}

int KeyValueStoreStateMachine::applyDelta(const KeyValueStoreDelta& delta,
                                          KeyValueStoreState& state,
                                          lsn_t version,
                                          std::chrono::milliseconds,
                                          std::string& failure_reason) {
  auto type = delta.getType();
  switch (type) {
    case KeyValueStoreDelta::Type::update_value: {
      state.store[delta.get_update_value().key] =
          delta.get_update_value().value;
      state.set_version(version);
      return 0;
    }
    case KeyValueStoreDelta::Type::remove_value: {
      // TODO: Not implemented
      return 0;
    }
    default:
      ld_warning("Unknown type of KeyValueStoreDelta. Not applying the delta");
      failure_reason = "Unknown type";
      return -1;
  }
}

int KeyValueStoreStateMachine::serializeState(const KeyValueStoreState& state,
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

bool KeyValueStoreStateMachine::shouldCreateSnapshot() const {
  // TODO: Not implemented
  return false;
}
bool KeyValueStoreStateMachine::canSnapshot() const {
  // TODO: Not implemented
  return false;
}
void KeyValueStoreStateMachine::onSnapshotCreated(Status, size_t) {
  // TODO: Not implemented
}
}} // namespace facebook::logdevice
