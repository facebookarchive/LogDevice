/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 facebook.logdevice.membership.thrift
namespace py3 logdevice.membership

typedef byte (cpp2.type = "std::uint8_t") u8
typedef i16 (cpp2.type = "std::uint16_t") u16
typedef i32 (cpp2.type = "std::uint32_t") u32
typedef i64 (cpp2.type = "std::uint64_t") u64

typedef u16 node_idx

///////////// StorageMembership ///////////////////

// keep consistent with logdevice/common/membership/StorageState.h
enum StorageState  {
  NONE = 0,
  NONE_TO_RO = 1,
  READ_ONLY = 2,
  READ_WRITE = 3,
  RW_TO_RO = 4,
  DATA_MIGRATION = 5,
  PROVISIONING = 7,
  INVALID = 6,
}

enum MetaDataStorageState {
  NONE = 0,
  METADATA = 1,
  PROMOTING = 2,
  INVALID = 3,
}

struct ShardID {
  1: u16 node_idx;
  2: u16 shard_idx;
}

struct ShardState {
  1: u16 shard_idx;
  2: StorageState storage_state;
  3: u32 flags;
  4: MetaDataStorageState metadata_state;
  5: u64 active_maintenance;
  6: u64 since_version;
}

typedef map<node_idx, list<ShardState>> StorageNodeState

struct StorageMembership {
  1: u32 proto_version;
  2: u64 membership_version;
  3: StorageNodeState node_states;
  4: list<ShardID> metadata_shards;
  5: bool bootstrapping;
}

////////////////// Sequencer Membership ////////////////

struct SequencerNodeState {
  1: double weight;
  2: u64 active_maintenance;
  3: bool sequencer_enabled;
}

struct SequencerMembership {
  1: u32 proto_version;
  2: u64 membership_version;
  3: map<node_idx, SequencerNodeState> node_states;
  4: bool bootstrapping;
}
