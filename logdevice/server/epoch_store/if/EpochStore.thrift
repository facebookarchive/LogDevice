/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/common/if/common.thrift"

namespace cpp2 facebook.logdevice.epoch_store.thrift

typedef byte (cpp2.type = "std::uint8_t") u8
typedef i16 (cpp2.type = "std::uint16_t") u16
typedef i32 (cpp2.type = "std::uint32_t") u32
typedef i64 (cpp2.type = "std::uint64_t") u64

// TODO(mbassem): Move to the common thrift defs
struct ShardID {
  1: common.NodeIndex node_idx;
  2: common.ShardIndex shard_idx;
}

// TailRecord holds information about the tail of the log.
struct TailRecord {
  1: u64 logid;
  2: u64 lsn;
  3: u64 timestamp;
  4: u32 flags_value;
  5: map<u8, u64> offset_map;
}

// TODO(mbassem): Move to the common thrift defs
typedef list<ShardID> StorageSet

// See documentation of EpochMetaData in EpochMetaData.h
struct EpochReplicationConfig {
  1: u32 effective_since;
  2: u64 creation_timestamp;
  3: list<ShardID> storage_set;
  4: common.ReplicationProperty replication;
  5: optional list<double> weights;
  6: bool written_in_metadatalog;
}

// See documentation of EpochMetaData::NodeSetParams in EpochMetaData.h
struct NodeSetParams {
  1: u64 signature;
  2: u64 seed;
  3: u16 target_nodeset_size;
}

struct LogMetaData {

  // The next epoch that will be used during sequencer activation.
  1: u32 next_epoch;

  // The index of the node that incremented the epoch number.
  2: optional u16 epoch_incremented_by;

  // The generation of the node that incremented the epoch number.
  // TODO: Drop the generation when thrift is the source of truth.
  3: u16 epoch_incremented_by_generation;

  // Timestamp in milliseconds at which the epoch was incremented.
  4: common.Timestamp epoch_incremented_at_ms;

  // The current replication config used by the running sequencer.
  5: EpochReplicationConfig current_replication_config;

  // LCE Information for data log.
  6: u32 data_last_clean_epoch;
  7: TailRecord data_tail_record;

  // LCE Information for metadata log.
  8: u32 metadata_last_clean_epoch;
  9: TailRecord metadata_tail_record;

  // Information used to generate the current storage set. Used by nodeset
  // generators to check whether a new nodeset needs to be generated or not.
  10: optional NodeSetParams nodeset_params;

  // Whether the log is disabled or not. Sequencers will refuse to activate as
  // long as this value is set to false.
  11: bool disabled;

  // A monotonically increasing version that gets bumped whenever any of the
  // information in this struct is changed.
  12: u64 version;

  // Timestamp in milliseconds at which this structure is changed.
  13: common.Timestamp last_modified_at_ms;
}
