/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"
#include "logdevice/common/replicated_state_machine/if/gen-cpp2/KeyValueStore_types.h"

namespace facebook { namespace logdevice {

/**
 * KeyValueStoreStateMachine is a replicated state machine that maintains the
 * key value store state in memory. The structure of the state and the delta is
 * documented in KeyValueStore.thrift file. For detailed information on how this
 * works, check out the documentation on ReplicatatedStateMachine.h
 */
class KeyValueStoreStateMachine
    : public facebook::logdevice::ReplicatedStateMachine<
          replicated_state_machine::thrift::KeyValueStoreState,
          replicated_state_machine::thrift::KeyValueStoreDelta> {
 public:
  explicit KeyValueStoreStateMachine(logid_t delta_log_id,
                                     logid_t snapshot_log_id = LOGID_INVALID);

  std::unique_ptr<replicated_state_machine::thrift::KeyValueStoreState>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<replicated_state_machine::thrift::KeyValueStoreState>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

  std::unique_ptr<replicated_state_machine::thrift::KeyValueStoreDelta>
  deserializeDelta(Payload payload) override;

  int applyDelta(
      const replicated_state_machine::thrift::KeyValueStoreDelta& delta,
      replicated_state_machine::thrift::KeyValueStoreState& state,
      lsn_t version,
      std::chrono::milliseconds timestamp,
      std::string& failure_reason) override;

  int serializeState(
      const replicated_state_machine::thrift::KeyValueStoreState& state,
      void* buf,
      size_t buf_size) override;

  bool shouldCreateSnapshot() const override;
  bool canSnapshot() const override;
  void onSnapshotCreated(Status st, size_t snapshotSize) override;
};
}} // namespace facebook::logdevice
