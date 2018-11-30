/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include <folly/Conv.h>
#include <folly/Optional.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Durability.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file  Local log store write operations.
 */

/**
 * Types of write operations supported.
 */
enum class WriteType : char {
  PUT,
  DELETE,
  DUMP_RELEASE_STATE,
  PUT_LOG_METADATA,
  DELETE_LOG_METADATA,
  PUT_SHARD_METADATA,
  MERGE_MUTABLE_PER_EPOCH_METADATA
};

inline std::string toString(WriteType wt) {
  switch (wt) {
    case WriteType::PUT:
      return "PUT";
    case WriteType::DELETE:
      return "DELETE";
    case WriteType::DUMP_RELEASE_STATE:
      return "DUMP_RELEASE_STATE";
    case WriteType::PUT_LOG_METADATA:
      return "PUT_LOG_METADATA";
    case WriteType::PUT_SHARD_METADATA:
      return "PUT_SHARD_METADATA";
    case WriteType::MERGE_MUTABLE_PER_EPOCH_METADATA:
      return "MERGE_MUTABLE_PER_EPOCH_METADATA";
    case WriteType::DELETE_LOG_METADATA:
      return "DELETE_LOG_METADATA";
  }
  return "UNKNOWN";
}

/**
 * Interface that represtent one log store write operation. Part of a batch
 * of writes passed to writeMulti().
 */
class WriteOp {
 public:
  virtual WriteType getType() const = 0;

  /**
   * The storage durability requirement for this operation.
   */
  virtual Durability durability() const = 0;

  virtual FlushToken syncToken() const {
    return FlushToken_INVALID;
  }

  virtual void setFlushToken(FlushToken /* unused */) const {}

  virtual FlushToken flushToken() const {
    return FlushToken_INVALID;
  }

  virtual std::string toString() const = 0;

  /**
   * Require a more durable storage type than originally requested.
   * Used when the storage layer cannot achieve correctness when
   * using the requested durability type.
   */
  virtual void
  increaseDurabilityTo(Durability /*min_durability*/,
                       FlushToken /*sync_token*/ = FlushToken_INVALID) const {}

  virtual ~WriteOp() {}
};

/**
 * Write operation for a record with a given (log_id, lsn).
 */
struct RecordWriteOp : public WriteOp {
  RecordWriteOp() {}

  RecordWriteOp(logid_t log_id, lsn_t lsn) : log_id(log_id), lsn(lsn) {}

  logid_t log_id;
  lsn_t lsn;
};

/**
 * STORE record operation
 */
struct PutWriteOp : public RecordWriteOp {
  PutWriteOp() {}

  PutWriteOp(logid_t log_id,
             lsn_t lsn,
             const Slice& record_header,
             const Slice& data,
             folly::Optional<node_index_t> coordinator,
             folly::Optional<lsn_t> block_starting_lsn,
             const Slice& copyset_index_entry,
             std::vector<std::pair<char, std::string>> index_key_list,
             Durability durability,
             bool is_rebuilding)
      : RecordWriteOp(log_id, lsn),
        record_header(record_header),
        data(data),
        coordinator(coordinator),
        copyset_index_lsn(block_starting_lsn),
        copyset_index_entry(copyset_index_entry),
        index_key_list(index_key_list),
        durability_(durability),
        is_rebuilding_(is_rebuilding) {}

  // this version of the constructor is only used in tests
  PutWriteOp(logid_t log_id, lsn_t lsn, const Slice& record_header)
      : RecordWriteOp(log_id, lsn),
        record_header(record_header),
        data(),
        durability_(Durability::ASYNC_WRITE),
        sync_token_(FlushToken_INVALID),
        is_rebuilding_(false) {}

  WriteType getType() const override {
    return WriteType::PUT;
  }

  Durability durability() const override {
    return durability_;
  }

  FlushToken syncToken() const override {
    return sync_token_;
  }

  std::string toString() const override {
    LocalLogStoreRecordFormat::flags_t flags;
    uint32_t wave;
    copyset_size_t copyset_size;
    std::chrono::milliseconds timestamp;
    esn_t last_known_good;
    OffsetMap offsets_within_epoch;
    int rv = LocalLogStoreRecordFormat::parse(record_header,
                                              &timestamp,
                                              &last_known_good,
                                              &flags,
                                              &wave,
                                              &copyset_size,
                                              nullptr,
                                              0,
                                              &offsets_within_epoch,
                                              nullptr,
                                              nullptr,
                                              -1 /* unused */);
    if (rv == -1) {
      return std::string("write type: put, log id: ") +
          std::to_string(log_id.val_) + std::string(", lsn: ") +
          lsn_to_string(lsn) + std::string(", is rebuilding: ") +
          std::to_string(isRebuilding()) + std::string(", header: ") +
          hexdump_buf(record_header, 256).c_str();
    }
    return std::string("write type: put, log id: ") +
        std::to_string(log_id.val_) + std::string(", lsn: ") +
        lsn_to_string(lsn) + std::string(", wave: ") + std::to_string(wave) +
        std::string(", durability: ") + durability_to_string(durability()) +
        std::string(", is rebuilding: ") + std::to_string(isRebuilding()) +
        std::string(", timestamp: ") + format_time(timestamp) +
        std::string(", last known good: ") +
        std::to_string(last_known_good.val_) + std::string(", flags: ") +
        LocalLogStoreRecordFormat::flagsToString(flags) +
        std::string(", copyset size: ") + std::to_string(copyset_size) +
        std::string(", offsets within epoch: ") +
        offsets_within_epoch.toString();
  }

  std::map<KeyType, std::string> getKeys() const {
    std::map<KeyType, std::string> keys;
    int rv = LocalLogStoreRecordFormat::parse(record_header,
                                              nullptr, // timeout
                                              nullptr, // last_known_good
                                              nullptr, // flags
                                              nullptr, // wave
                                              nullptr, // copyset_size
                                              nullptr, // copyset_arr
                                              0,       // copyset_arr_size
                                              nullptr, // offsets_within_epoch
                                              &keys,   // optional_keys
                                              nullptr, // payload_out
                                              -1);     // shard (unused)

    ld_check(rv == 0);

    return keys;
  }

  bool isRebuilding() const {
    return is_rebuilding_;
  }

  void increaseDurabilityTo(
      Durability min_durability,
      FlushToken sync_token = FlushToken_INVALID) const override {
    durability_ = std::max(durability_, min_durability);
    sync_token_ = std::max(sync_token_, sync_token);
  }

  FlushToken flushToken() const override {
    return flush_token_;
  }

  void setFlushToken(FlushToken flush_token) const override {
    flush_token_ = flush_token;
  }

  Slice record_header;
  Slice data;
  // The node that is orchestrating this operation. This coordinator
  // is considered the "source" for the operation. Should the write
  // be lost (e.g. due to it still being buffered in memory at the
  // time of a system failure), the coordinator,destination pair is
  // used to limit the amount of data that is rebuilt.
  folly::Optional<node_index_t> coordinator;
  folly::Optional<lsn_t> copyset_index_lsn;
  Slice copyset_index_entry;
  std::vector<std::pair<char, std::string>> index_key_list;

  // Used in unit tests. Don't change this value unless you're a unit test.
  DataKeyFormat TEST_data_key_format = DataKeyFormat::DEFAULT;

 private:
  // Durability can be increased by the storage layer if a
  // less durable store is not supported or cannot be achieved
  // without impacting correctness.
  mutable Durability durability_;
  mutable FlushToken sync_token_;
  // Flush token will value greater than or equal to that of the memtable that
  // has the write.
  mutable FlushToken flush_token_;
  bool is_rebuilding_;
};

/**
 * DELETE record operation
 */
struct DeleteWriteOp : public RecordWriteOp {
  DeleteWriteOp() {}

  DeleteWriteOp(logid_t log_id, lsn_t lsn) : RecordWriteOp(log_id, lsn) {}

  WriteType getType() const override {
    return WriteType::DELETE;
  }

  Durability durability() const override {
    // DELETEs are only used as a best effort to clean up extra copies of
    // record that were sent out to reduce write latency.  We don't need to
    // sync them.
    return Durability::ASYNC_WRITE;
  }

  std::string toString() const override {
    return std::string("write type: delete, log id: ") +
        std::to_string(log_id.val_) + std::string(", lsn: ") +
        lsn_to_string(lsn);
  }
};

/**
 * Represents an operation to dump the contents of the release state map.
 * Entries are grouped and written in batch.
 */
struct DumpReleaseStateWriteOp : public WriteOp {
  using ReleaseStates = std::vector<std::pair<logid_t, lsn_t>>;

  DumpReleaseStateWriteOp() {}
  explicit DumpReleaseStateWriteOp(ReleaseStates&& releases)
      : releases_(std::move(releases)){};

  WriteType getType() const override {
    return WriteType::DUMP_RELEASE_STATE;
  }

  Durability durability() const override {
    return Durability::ASYNC_WRITE;
  }

  std::string toString() const override {
    std::string releases_states_str = "";
    for (size_t i = 0; i < releases_.size(); ++i) {
      auto release = releases_[i];
      releases_states_str.append("(logid: ");
      releases_states_str.append(std::to_string(release.first.val_));
      releases_states_str.append(", lsn: ");
      releases_states_str.append(lsn_to_string(release.second));
      releases_states_str.append("), ");
    }
    return std::string("write type: dump release state, release states: ") +
        releases_states_str;
  }

  ReleaseStates releases_;
};

struct PutLogMetadataWriteOp : public WriteOp {
  PutLogMetadataWriteOp() {}

  PutLogMetadataWriteOp(logid_t log_id,
                        LogMetadata* metadata,
                        Durability durability)
      : log_id_(log_id), metadata_(metadata), durability_(durability) {}

  WriteType getType() const override {
    return WriteType::PUT_LOG_METADATA;
  }

  Durability durability() const override {
    return durability_;
  }

  std::string toString() const override {
    return std::string("write type: put log metadata, log id: ") +
        std::to_string(log_id_.val_) + std::string(", durability: ") +
        durability_to_string(durability());
  }

  logid_t log_id_;
  LogMetadata* metadata_;

 private:
  Durability durability_;
};

struct DeleteLogMetadataWriteOp : public WriteOp {
  DeleteLogMetadataWriteOp(logid_t first_log_id,
                           logid_t last_log_id,
                           const LogMetadataType& type)
      : first_log_id_(first_log_id), last_log_id_(last_log_id), type_(type) {}

  WriteType getType() const override {
    return WriteType::DELETE_LOG_METADATA;
  }

  Durability durability() const override {
    return Durability::ASYNC_WRITE;
  }

  std::string toString() const override {
    return std::string("write type: delete log metadata, starting log id: ") +
        folly::to<std::string>(first_log_id_.val_) +
        std::string(", last log id: ") +
        folly::to<std::string>(last_log_id_.val_) +
        std::string(", log metadata type: ") + folly::to<std::string>(type_);
  }

  logid_t first_log_id_;
  logid_t last_log_id_;
  LogMetadataType type_;
};

struct PutStoreMetadataWriteOp : public WriteOp {
  PutStoreMetadataWriteOp() {}

  PutStoreMetadataWriteOp(StoreMetadata* metadata, Durability durability)
      : metadata_(metadata), durability_(durability) {}

  WriteType getType() const override {
    return WriteType::PUT_SHARD_METADATA;
  }

  Durability durability() const override {
    return durability_;
  }

  std::string toString() const override {
    return std::string("write type: put shard metadata") +
        std::string(", durability: ") + durability_to_string(durability());
  }

  StoreMetadata* metadata_;

 private:
  Durability durability_;
};

/**
 * Write operation for mutable per-epoch metadata.
 */
struct MergeMutablePerEpochLogMetadataWriteOp : public WriteOp {
  MergeMutablePerEpochLogMetadataWriteOp() {}

  MergeMutablePerEpochLogMetadataWriteOp(
      logid_t log_id,
      epoch_t epoch,
      const MutablePerEpochLogMetadata* metadata)
      : log_id(log_id), epoch(epoch), metadata(metadata) {}

  WriteType getType() const override {
    return WriteType::MERGE_MUTABLE_PER_EPOCH_METADATA;
  }

  Durability durability() const override {
    return Durability::ASYNC_WRITE;
  }

  std::string toString() const override {
    return std::string("write type: put mutable per-epoch metadata, log_id: ") +
        std::to_string(log_id.val_) + std::string(", durability: ") +
        durability_to_string(durability());
    std::string(", epoch: ") + std::to_string(epoch.val_) +
        std::string(", metadata: ") + metadata->toString();
  }

  logid_t log_id;
  epoch_t epoch;
  const MutablePerEpochLogMetadata* metadata;
};

}} // namespace facebook::logdevice
