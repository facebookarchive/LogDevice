/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/small_vector.h>
#include <rocksdb/slice.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/NodeDirtyData.h"

namespace facebook { namespace logdevice {

/**
 * Metadata associated with a partition.
 */
enum class PartitionMetadataType {
  STARTING_TIMESTAMP = 0,
  MIN_TIMESTAMP = 1,
  MAX_TIMESTAMP = 2,
  LAST_COMPACTION = 3,
  COMPACTED_RETENTION = 4,
  DIRTY = 5,
  MAX
};

class PartitionMetadata : public Metadata {
 public:
  /**
   * @return  the type of metadata this class represents.
   */
  virtual PartitionMetadataType getType() const = 0;
};

// STARTING_TIMESTAMP, MIN_TIMESTAMP, MAX_TIMESTAMP, LAST_COMPACTION.
class PartitionTimestampMetadata final : public PartitionMetadata {
 public:
  explicit PartitionTimestampMetadata(
      PartitionMetadataType type,
      RecordTimestamp timestamp = RecordTimestamp(std::chrono::milliseconds(0)))
      : timestamp_(timestamp.toMilliseconds().count()), type_(type) {
    ld_check(type_ == PartitionMetadataType::STARTING_TIMESTAMP ||
             type_ == PartitionMetadataType::MIN_TIMESTAMP ||
             type_ == PartitionMetadataType::MAX_TIMESTAMP ||
             type_ == PartitionMetadataType::LAST_COMPACTION);
  }

  PartitionMetadataType getType() const override {
    return type_;
  }

  GEN_METADATA_SERIALIZATION_METHODS(
      PartitionTimestampMetadata,
      timestamp_,
      format_time(std::chrono::milliseconds(timestamp_)))

  RecordTimestamp getTimestamp() const {
    return RecordTimestamp::from(std::chrono::milliseconds(timestamp_));
  }
  void setTimestamp(RecordTimestamp timestamp) {
    timestamp_ = timestamp.toMilliseconds().count();
  }

  uint64_t timestamp_;

 private:
  PartitionMetadataType type_;
};

// Presence of this metadata means that we have compacted the partition
// in order to get rid of records with backlog duration <= backlog_duration_.
// If it exists, we shouldn't try to compact it again for the same duration.
// This avoids an unlikely case of compacting an old partition over and over
// as old data gets written to it during rebuilding.
class PartitionCompactedRetentionMetadata final : public PartitionMetadata {
 public:
  explicit PartitionCompactedRetentionMetadata(
      std::chrono::seconds backlog_duration = std::chrono::seconds(0))
      : backlog_duration_(backlog_duration.count()) {
    ld_check(backlog_duration.count() >= 0);
  }

  PartitionMetadataType getType() const override {
    return PartitionMetadataType::COMPACTED_RETENTION;
  }

  GEN_METADATA_SERIALIZATION_METHODS(PartitionCompactedRetentionMetadata,
                                     backlog_duration_,
                                     std::to_string(backlog_duration_) + "ms")

  std::chrono::seconds getBacklogDuration() const {
    return std::chrono::seconds(backlog_duration_);
  }

  uint64_t backlog_duration_;
};

class PartitionDirtyMetadata : public PartitionMetadata {
 public:
  using DirtyNodeVector = std::vector<node_index_t>;
  using DirtyNodeVectors = std::array<DirtyNodeVector, (size_t)DataClass::MAX>;

  explicit PartitionDirtyMetadata() {}
  explicit PartitionDirtyMetadata(const DirtiedByMap&, bool under_replicated);

  PartitionMetadataType getType() const override {
    return PartitionMetadataType::DIRTY;
  }

  Slice serialize() const override;
  int deserialize(Slice blob) override;
  std::string toString() const override;

  bool isUnderReplicated() const {
    return under_replicated_;
  }

  const DirtyNodeVector& getDirtiedBy(DataClass dc) const {
    ld_check(dc < DataClass::MAX);
    return dirtied_by_[(size_t)dc];
  }

  const DirtyNodeVectors& getAllDirtiedBy() const {
    return dirtied_by_;
  }

 private:
  // Offsets are from the beginning of the metadata record.
  // Both offsets and lengths are in terms of bytes.
  struct DirtyNodesForClass {
    // Length of this structure exclusive of any data it points to.
    // i.e. sizeof(DirtyNodesForClass). Used for forward/backward compatibility.
    uint16_t len;
    uint8_t data_class;
    uint8_t pad = 0;
    uint32_t nid_array_offset;
    uint32_t nid_array_len;
  };
  static_assert(sizeof(DirtyNodesForClass) == 12,
                "PartitionDirtyMetadata::DirtyNodesForClass is not packed.");

  struct Header {
    using flags_t = uint8_t;

    // Some writes to this partition were lost for this partition
    // (e.g. due to LogDevice crashing) and these records have not
    // yet been rebuilt.
    static const flags_t UNDER_REPLICATED = (unsigned)1 << 0;

    // Length of this structure exclusive of any data it points to.
    // i.e. sizeof(Header). Used for forward/backward compatibility.
    uint16_t len;
    flags_t flags;
    uint8_t pad = 0;
    // Offset to DirtyNodeForClass elements
    uint32_t dnc_array_offset;
    uint32_t dnc_array_len;
  };
  static_assert(sizeof(Header) == 12,
                "PartitionDirtyMetadata::Header is not packed.");

  DirtyNodeVectors dirtied_by_;

  // This partition has lost records that have not yet been restored by
  // rebuilding.
  bool under_replicated_ = false;

  mutable std::vector<uint8_t> serialize_buffer_;
};

/**
 * Value of partition directory entry. Contains the maximum LSN for this log
 * in this partition. May be slightly overestimated in rare cases.
 *
 * For historical reasons, this starts with 8 bytes that are now unused and
 * being phased out.
 *
 * Format summary:
 * writes:  (approximate_size_bytes, max_lsn, flags)
 * accepts: (approximate_size_bytes, max_lsn, flags), or
 *          (approximate_size_bytes, max_lsn, min_ts, max_ts, flags)
 */
class PartitionDirectoryValue {
 public:
  using flags_t = uint8_t;

  // The entry was written by a non-durable write to rocksdb
  // and may only exist in memory.
  static const flags_t NOT_DURABLE = (unsigned)1 << 0;
  // Any records in the partition for this log are metadata records, such as
  // bridge records.
  static const flags_t PSEUDORECORDS_ONLY = (unsigned)1 << 1;

  static std::string flagsToString(flags_t flags) {
    std::string res = "";
    if (NOT_DURABLE & flags) {
      res += "NOT_DURABLE";
    }
    if (PSEUDORECORDS_ONLY & flags) {
      if (!res.empty()) {
        res += "|";
      }
      res += "PSEUDORECORDS_ONLY";
    }
    return res;
  }

  explicit PartitionDirectoryValue(lsn_t max_lsn,
                                   flags_t flags,
                                   size_t approximate_size_bytes)
      : approximate_size_bytes_(approximate_size_bytes),
        max_lsn_(max_lsn),
        flags_(flags) {}

  /**
   * Extracts the max_lsn from a memory segment that is
   * a PartitionDirectoryValue.
   */
  static lsn_t getMaxLSN(const void* blob, size_t size) {
    ld_check(valid(blob, size));
    const void* ptr = reinterpret_cast<const char*>(blob) +
        offsetof(PartitionDirectoryValue, max_lsn_);
    lsn_t lsn;
    memcpy(&lsn, ptr, sizeof lsn);
    return lsn;
  }

  static flags_t getFlags(const void* blob, size_t size) {
    ld_check(valid(blob, size));
    return *(reinterpret_cast<const char*>(blob) + size - sizeof(flags_));
  }

  static size_t getApproximateSizeBytes(const void* blob, size_t size) {
    ld_check(valid(blob, size));

    const void* ptr = reinterpret_cast<const char*>(blob) +
        offsetof(PartitionDirectoryValue, approximate_size_bytes_);
    size_t result;
    memcpy(&result, ptr, sizeof result);

    if (result == RecordTimestamp::min().toMilliseconds().count()) {
      // 2.35 wrote this value as garbage since the field was being phased out;
      // just treat it as 0 if we ever encounter it.
      // TODO(T24193405): once 2.35 is long gone, remove this hack.
      result = 0;
    }
    return result;
  }

  /**
   * Checks if a memory segment of @param size bytes starting from @param blob
   * represents a valid PartitionDirectoryValue.
   *
   * The latter format is accepted for forward compatibility with future
   * releases (post T30754993), which will add timestamps.
   */
  static bool valid(const void* /*blob*/, size_t size) {
    return (size ==
            sizeof(approximate_size_bytes_) + sizeof(max_lsn_) +
                sizeof(flags_)) ||
        (size ==
         sizeof(approximate_size_bytes_) + sizeof(max_lsn_) + sizeof(uint64_t) +
             sizeof(uint64_t) + sizeof(flags_));
  }

  Slice serialize() const {
    return Slice(
        this, offsetof(PartitionDirectoryValue, flags_) + sizeof(flags_));
  }

  // Maintained to give an estimate as to how many bytes are in the partition
  // for the given log ID.
  uint64_t approximate_size_bytes_;

  lsn_t max_lsn_;
  // NOTE two uint64_t's will be added here, per T30754993, as accounted for in
  // getFlags and valid, so be careful if adding more fields while this comment
  // remains.
  flags_t flags_;

  // Document the tail padding that would otherwise be inserted by the
  // compiler to ensure all fields are naturally aligned even in an array
  // of PartitionDirectoryValues. This data is not serialized (note that
  // the serialization code does not use sizeof(PartitionDirectoryValue)),
  // and some or all of this space can be used by fields added to future
  // versions of this structure so long as their use is gated by a bit in
  // flags_.
  uint8_t alignment_tail_padding_[7];
};

// Verify there is no compiler inserted, inter-field padding.
static_assert(sizeof(PartitionDirectoryValue) ==
                  (sizeof(PartitionDirectoryValue::approximate_size_bytes_) +
                   sizeof(PartitionDirectoryValue::max_lsn_) +
                   sizeof(PartitionDirectoryValue::flags_) +
                   sizeof(PartitionDirectoryValue::alignment_tail_padding_)),
              "A field in PartitionDirectoryValue is not naturally aligned");

/**
 * The value from the mapping written in the custom index to the metadata column
 * family. See CustomIndexDirectoryKey in RocksDBKeyFormat.h.
 */
namespace CustomIndexDirectoryValue {

inline folly::small_vector<char, 18> create(rocksdb::Slice key, lsn_t lsn) {
  ld_check(key.size() <= USHRT_MAX);

  folly::small_vector<char, 18> value(sizeof(ushort) + key.size() +
                                      sizeof(lsn_t));

  unsigned short key_length = key.size();
  memcpy(&value[0], &key_length, sizeof(ushort));

  int offset = sizeof(ushort);
  memcpy(&value[offset], key.data(), key.size());

  offset += key.size();
  memcpy(&value[offset], &lsn, sizeof(lsn_t));

  offset += sizeof(lsn_t);
  ld_check(offset == value.size());
  return value;
}

/**
 * Extracts the key from a memory segment that is a CustomIndexDirectoryValue.
 */
inline rocksdb::Slice getKey(const void* blob) {
  unsigned short length;
  memcpy(&length, blob, sizeof(ushort));

  const void* ptr = reinterpret_cast<const char*>(blob) + sizeof(ushort);
  return rocksdb::Slice(reinterpret_cast<const char*>(ptr), length);
}

/**
 * Extracts the LSN from a memory segment that is a CustomIndexDirectoryValue.
 */
inline lsn_t getLSN(const void* blob, size_t size) {
  const size_t offset = size - sizeof(lsn_t);
  const void* ptr = reinterpret_cast<const char*>(blob) + offset;
  lsn_t lsn;
  memcpy(&lsn, ptr, sizeof lsn);
  return lsn;
}

/**
 * Checks if a memory segment of @param size bytes starting from @param blob
 * represents a valid CustomIndexDirectoryValue.
 */
inline bool valid(const void* blob, size_t size) {
  if (size < sizeof(ushort) + sizeof(lsn_t)) {
    return false;
  }

  unsigned short key_length;
  memcpy(&key_length, blob, sizeof(ushort));
  return size == sizeof(ushort) + key_length + sizeof(lsn_t);
}

/**
 * Checks whether the CustomIndexDirectoryValue corresponding to @param val1
 * is smaller than the CustomIndexDirectoryValue corresponding to @param val2.
 */
inline bool compare(rocksdb::Slice val1, rocksdb::Slice val2) {
  rocksdb::Slice val1_key = getKey(val1.data());
  rocksdb::Slice val2_key = getKey(val2.data());
  int res = memcmp(val1_key.data(),
                   val2_key.data(),
                   std::min(val1_key.size(), val2_key.size()));
  if (res < 0) {
    return true;
  }
  if (res == 0 &&
      (val1_key.size() < val2_key.size() ||
       getLSN(val1.data(), val1.size()) < getLSN(val2.data(), val2.size()))) {
    return true;
  }
  return false;
}
} // namespace CustomIndexDirectoryValue

}} // namespace facebook::logdevice
