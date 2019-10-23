/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.replicated_state_machine.thrift

typedef i64 (cpp2.type = "std::uint64_t") u64

struct UpdateValue {
  1: string key;
  2: binary value;
}

struct RemoveValue {
  1: string key;
}

/**
 * The representation of delta in KeyValueStoreStateMachine.
 */
union KeyValueStoreDelta {
  1: UpdateValue update_value;
  2: RemoveValue remove_value;
}

/**
 * The representation of state in KeyValueStoreStateMachine.
 */
struct KeyValueStoreState {
  1: map<string, binary> store;
  2: u64 version;
}
