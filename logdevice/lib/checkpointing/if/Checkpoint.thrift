/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.checkpointing.thrift

typedef i64 (cpp2.type = "std::uint64_t") u64

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
