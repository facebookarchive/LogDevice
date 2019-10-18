/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.checkpointing.thrift

typedef i64 (cpp2.type = "std::uint64_t") u64

struct UpdateCheckpoint {
  1: string customer_id;
  2: Checkpoint checkpoint;
}

struct RemoveCheckpoint {
  1: string customer_id;
}

/**
 * The represenation of delta in CheckpointStateMachine.
 */
union CheckpointDelta {
  1: UpdateCheckpoint update_checkpoint;
  2: RemoveCheckpoint remove_checkpoint;
}

/**
 * The represenation of state in CheckpointStateMachine.
 */
struct CheckpointState {
  /*
   * The map where the key is customer id and the value is checkpoints for all
   * logs of this customer.
   */
  1: map<string, Checkpoint> checkpoints;
  2: u64 version;
}

/**
 * A data structure representing a value stored in CheckpointStore map.
 */
struct Checkpoint {
  /**
   * The map where the key is log_id and the value is lsn.
   */
  1: map<u64, u64> log_lsn_map;
  2: u64 version;
}
