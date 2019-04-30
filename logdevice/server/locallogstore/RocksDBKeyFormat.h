/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstring>

#include <folly/CppAttributes.h>
#include <folly/small_vector.h>
#include <rocksdb/slice.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/PartitionMetadata.h"

#ifdef __clang__
// Without this pragma, clang emits "private field 'header' is not used" which
// we don't care for
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif

namespace facebook { namespace logdevice { namespace RocksDBKeyFormat {

/**
 * KeyPrefix is an enum with the list of prefixes to be used in RocksDB.
 *
 *                 KEEP THE FOLLOWING LIST SORTED TO PREVENT
 *                    FROM USING THE SAME CHARACTER TWICE.
 */
enum class KeyPrefix : char {
  LogSnapshotBlob = ']',
  // TODO: (T30250351) clean-up that deprecated metadata everywhere so we can
  // remove this from the enum.
  Deprecated_1 = 'a',
  StoreMeta_RebuildingComplete = 'b',
  CopySetIndex = 'C',
  LogMeta_LastClean = 'c',
  PartitionMeta_Dirty = 'D',
  DataKey = 'd',
  LogMeta_LogRemovalTime = 'e',
  Index = 'I',
  LogMeta_SoftSeal = 'i',
  StoreMeta_ClusterMarker = 'i',
  // TODO(T23728838): fix the collision with LogMeta_SoftSeal
  CustomIndexDirectory = 'M',
  LogMeta_LastReleased = 'm',
  // NOTE 'o' is deprecated and removed from shards on startup from now on
  PartitionDirectory = 'p',
  StoreMeta_RebuildingRanges = 'q',
  LogMeta_RebuildingCheckpoint = 'R',
  Deprecated_2 = 'r',
  LogMeta_Seal = 's',
  LogMeta_TrimPoint = 't',
  Deprecated_3 = 'u',
  PerEpochLogMeta_Recovery = 'V',
  PartitionMeta_StartingTimestamp = 'v',
  PerEpochLogMeta_Mutable = 'W',
  PartitionMeta_CompactedRetention = 'w',
  PartitionMeta_MinTimestamp = 'x',
  PartitionMeta_MaxTimestamp = 'y',
  PartitionMeta_LastCompaction = 'z',
  // Reserved for identifying the version of DB format, see checkSchemaVersion()
  Reserved_SchemaVersion = '.'
};

constexpr char prefix(KeyPrefix prefix) {
  return static_cast<char>(prefix);
}

/**
 * A DataKey is (almost) directly written (in binary) to rocksdb and read back.
 * The class ensures that the bytes are written in big endian in the correct
 * order, so that rocksdb lexicographical sorting works like we want it - by
 * log ID first, then LSN.
 *
 * A migration is in progress for DataKey's format.
 * The old format was (header, log_id, lsn, wave), the new format is
 * (header, log_id, lsn), i.e. we're getting rid of the wave in key because wave
 * is now in value instead.
 * The current state is that we're writing the new format but accept both old
 * and new.
 * TODO (#10357210):
 *   After all old-format data was trimmed (which may take a long time for
 *   metadata logs), stop accepting the old format. The write side was switched
 *   to new format around October 2018.
 *
 * Until everything is converted, different operations should use slightly
 * different key. Use the appropriate sliceFor*() method.
 */
class DataKey {
 public:
  using Format = DataKeyFormat;

  DataKey(logid_t log_id, lsn_t lsn) {
    log_id_big_endian_ = htobe64(log_id.val_);
    lsn_big_endian_ = htobe64(lsn);
  }

  // TODO (#10357210):
  //   After all data is converted, get rid of these sliceFor*() methods
  //   and have everyone just do
  //   rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)).

  // The smallest possible slice representing the same (log_id, lsn) pair.
  // Use it for iterator Seek().
  rocksdb::Slice sliceForForwardSeek() {
    return rocksdb::Slice(reinterpret_cast<const char*>(this),
                          offsetof(DataKey, wave_DEPRECATED_));
  }
  // The biggest possible slice representing the same (log_id, lsn) pair.
  // Use it for iterator SeekForPrev().
  rocksdb::Slice sliceForBackwardSeek() {
    wave_DEPRECATED_ = WAVE_MAX;
    return rocksdb::Slice(reinterpret_cast<const char*>(this), sizeof(DataKey));
  }
  // Use this for writing new records.
  rocksdb::Slice sliceForWriting(Format fmt = Format::DEFAULT) {
    // Unless overridden by tests, include wave into newly written records
    // because older versions don't understand the new format.
    wave_DEPRECATED_ = 0xeeeeeeeeu;
    return rocksdb::Slice(reinterpret_cast<const char*>(this),
                          fmt != Format::OLD
                              ? offsetof(DataKey, wave_DEPRECATED_)
                              : sizeof(DataKey));
  }
  // Returns the key in the old and new format.
  // Delete both of these keys when deleting the record.
  std::array<rocksdb::Slice, 2> slicesForDeleting() {
    wave_DEPRECATED_ = 0xeeeeeeeeu;
    return {
        rocksdb::Slice(reinterpret_cast<const char*>(this),
                       offsetof(DataKey, wave_DEPRECATED_)),
        rocksdb::Slice(reinterpret_cast<const char*>(this), sizeof(DataKey))};
  }

  static size_t sizeForWriting() {
    return offsetof(DataKey, wave_DEPRECATED_);
  }

  static bool isInNewFormat(const void* blob, size_t size) {
    return size == offsetof(DataKey, wave_DEPRECATED_);
  }

  /**
   * Extracts the log ID from a memory segment that is a DataKey.
   */
  static logid_t getLogID(const void* blob) {
    const size_t offset = offsetof(DataKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    // Use memcpy (instead of casting to uint64_t*) to avoid undefined
    // behaviour due to unaligned access.  GCC and Clang optimize the
    // fixed-size memcpy to a mov on x64.
    uint64_t raw_be;
    static_assert(sizeof(logid_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return logid_t(int64_t(be64toh(raw_be)));
  }

  /**
   * Extracts the LSN from a memory segment that is a DataKey.
   */
  static lsn_t getLSN(const void* blob) {
    const size_t offset = offsetof(DataKey, lsn_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(lsn_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return lsn_t(be64toh(raw_be));
  }

  /**
   * Checks if a memory segment of @param size bytes starting from @param blob
   * represents a valid DataKey.
   */
  static bool valid(const void* blob, size_t size) {
    return size >= offsetof(DataKey, wave_DEPRECATED_) &&
        reinterpret_cast<const char*>(blob)[0] == HEADER;
  }

  static constexpr char HEADER = prefix(KeyPrefix::DataKey);

  // length of the prefix containing just the log id
  static constexpr size_t PREFIX_LENGTH = sizeof(char) + sizeof(uint64_t);

 private:
  char header = HEADER;
  uint64_t log_id_big_endian_;
  uint64_t lsn_big_endian_;
  // There used to be wave here, but now it's in value instead of key.
  uint32_t wave_DEPRECATED_;
} __attribute__((__packed__));

/**
 * RocksDB key used when storing log metadata. @see DataKey
 */
class LogMetaKey {
 public:
  explicit LogMetaKey(LogMetadataType type, logid_t log_id)
      : header_(getHeader(type)), log_id_big_endian_(htobe64(log_id.val_)) {}

  // returns a key prefix for the given LogMetadataType
  static char getHeader(LogMetadataType type) {
    switch (type) {
      // TODO: (T30250351) clean-up that deprecated metadata everywhere so we
      // can remove this from the enum.
      case LogMetadataType::DEPRECATED_1:
      case LogMetadataType::DEPRECATED_2:
      case LogMetadataType::DEPRECATED_3:
        assert(false);
        std::abort();
        return 0;
      case LogMetadataType::LAST_RELEASED:
        return prefix(KeyPrefix::LogMeta_LastReleased);
      case LogMetadataType::TRIM_POINT:
        return prefix(KeyPrefix::LogMeta_TrimPoint);
      case LogMetadataType::SEAL:
        return prefix(KeyPrefix::LogMeta_Seal);
      case LogMetadataType::LAST_CLEAN:
        return prefix(KeyPrefix::LogMeta_LastClean);
      case LogMetadataType::REBUILDING_CHECKPOINT:
        return prefix(KeyPrefix::LogMeta_RebuildingCheckpoint);
      case LogMetadataType::SOFT_SEAL:
        return prefix(KeyPrefix::LogMeta_SoftSeal);
      case LogMetadataType::LOG_REMOVAL_TIME:
        return prefix(KeyPrefix::LogMeta_LogRemovalTime);
      // Before adding a new character type, search in the
      // file(this is not the only place where these characters are
      // defined)
      case LogMetadataType::MAX:
        break;
    }
    ld_check(false);
    return 0;
  }

  // Extracts the log ID from a memory segment that is a LogMetaKey.
  static logid_t getLogID(const void* blob) {
    const auto offset = offsetof(LogMetaKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(logid_t::raw_type) == sizeof(raw_be), "");
    memcpy(&raw_be, ptr, sizeof(raw_be));
    return logid_t(be64toh(raw_be));
  }

  // Checks if a memory segment represents a valid LogMetaKey of a given type.
  static bool valid(LogMetadataType type, const void* blob, size_t size) {
    return size >= sizeof(LogMetaKey) &&
        reinterpret_cast<const char*>(blob)[0] == getHeader(type);
  }

 private:
  char header_;
  uint64_t log_id_big_endian_;
} __attribute__((__packed__));

/**
 * RocksDB key used when storing per-epoch log metadata. @see DataKey
 */
class PerEpochLogMetaKey {
 public:
  static constexpr char HEADER_RECOVERY =
      prefix(KeyPrefix::PerEpochLogMeta_Recovery);
  static constexpr char HEADER_MUTABLE =
      prefix(KeyPrefix::PerEpochLogMeta_Mutable);

  explicit PerEpochLogMetaKey(PerEpochLogMetadataType type,
                              logid_t log_id,
                              epoch_t epoch)
      : header_(getHeader(type)),
        log_id_big_endian_(htobe64(log_id.val_)),
        epoch_big_endian_(htobe32(epoch.val_)) {}

  // returns a key prefix for the given PerEpochLogMetadataType
  static char getHeader(PerEpochLogMetadataType type) {
    switch (type) {
      case PerEpochLogMetadataType::RECOVERY:
        return HEADER_RECOVERY;
      case PerEpochLogMetadataType::MUTABLE:
        return HEADER_MUTABLE;
      case PerEpochLogMetadataType::MAX:
        break;
    }
    ld_check(false);
    return 0;
  }

  // Extracts the log ID from a memory segment that is a PerEpochLogMetaKey.
  static logid_t getLogID(const void* blob) {
    const auto offset = offsetof(PerEpochLogMetaKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(logid_t::raw_type) == sizeof(raw_be), "");
    memcpy(&raw_be, ptr, sizeof(raw_be));
    return logid_t(be64toh(raw_be));
  }

  // Extracts the epoch from a memory segment that is a PerEpochLogMetaKey.
  static epoch_t getEpoch(const void* blob) {
    const auto offset = offsetof(PerEpochLogMetaKey, epoch_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint32_t raw_be;
    static_assert(sizeof(epoch_t::raw_type) == sizeof(raw_be), "");
    memcpy(&raw_be, ptr, sizeof(raw_be));
    return epoch_t(be32toh(raw_be));
  }

  // Checks if a memory segment represents a valid PerEpochLogMetaKey of
  // a given type.
  static bool valid(PerEpochLogMetadataType type,
                    const void* blob,
                    size_t size) {
    return size >= sizeof(PerEpochLogMetaKey) &&
        reinterpret_cast<const char*>(blob)[0] == getHeader(type);
  }

  // Returns true if `valid(t, blob, size)' is true for some `t'.
  static bool validAnyType(const void* blob, size_t size) {
    if (size < sizeof(PerEpochLogMetaKey)) {
      return false;
    }
    char h = reinterpret_cast<const char*>(blob)[0];
    for (int itype = 0; itype < static_cast<int>(PerEpochLogMetadataType::MAX);
         ++itype) {
      if (h == getHeader(static_cast<PerEpochLogMetadataType>(itype))) {
        return true;
      }
    }
    return false;
  }

 private:
  char header_;
  uint64_t log_id_big_endian_;
  uint32_t epoch_big_endian_;
} __attribute__((__packed__));

/**
 * RocksDB key used when storing shard metadata. @see DataKey
 */
class StoreMetaKey {
 public:
  explicit StoreMetaKey(StoreMetadataType type) : header_(getHeader(type)) {
    ld_check(header_ != '\0');
  }

  // returns a key for the given StoreMetadataType
  static char getHeader(StoreMetadataType type) {
    switch (type) {
      case StoreMetadataType::CLUSTER_MARKER:
        return prefix(KeyPrefix::StoreMeta_ClusterMarker);
      case StoreMetadataType::REBUILDING_COMPLETE:
        return prefix(KeyPrefix::StoreMeta_RebuildingComplete);
      case StoreMetadataType::REBUILDING_RANGES:
        return prefix(KeyPrefix::StoreMeta_RebuildingRanges);
      case StoreMetadataType::UNUSED:
      case StoreMetadataType::MAX:
        break;
    }
    return '\0';
  }

 private:
  char header_;
} __attribute__((__packed__));

/**
 * Key of partition directory entry. Almost the same as DataKey.
 * Partition ID is included in key instead of value because in corner cases
 * there might be multiple partition ids for the same (log_id, lsn) pair.
 * The lsn is guaranteed to be less than or equal than LSNs of all records in
 * that partition; unless it's the first non-dropped partition for this log.
 * If a partition directory entry exists, it's very likely but
 * not guaranteed that the partition actually contains records for this log.
 * Partition IDs are guaranteed to non-decrease with increasing lsn
 * for the same log.
 */
class PartitionDirectoryKey {
 public:
  PartitionDirectoryKey(logid_t log_id,
                        lsn_t lsn,
                        partition_id_t partition_id) {
    log_id_big_endian_ = htobe64(log_id.val_);
    lsn_big_endian_ = htobe64(lsn);
    partition_id_big_endian_ = htobe64(partition_id);
  }

  /**
   * Extracts the log ID from a memory segment that is a PartitionDirectoryKey.
   */
  static logid_t getLogID(const void* blob) {
    const size_t offset = offsetof(PartitionDirectoryKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(logid_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return logid_t(int64_t(be64toh(raw_be)));
  }

  /**
   * Extracts the LSN from a memory segment that is a PartitionDirectoryKey.
   */
  static lsn_t getLSN(const void* blob) {
    const size_t offset = offsetof(PartitionDirectoryKey, lsn_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(lsn_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return lsn_t(be64toh(raw_be));
  }

  /**
   * Extracts the partition id from a memory segment that is a
   * PartitionDirectoryKey.
   */
  static partition_id_t getPartition(const void* blob) {
    const size_t offset =
        offsetof(PartitionDirectoryKey, partition_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    memcpy(&raw_be, ptr, sizeof raw_be);
    return be64toh(raw_be);
  }

  /**
   * Checks if a memory segment of @param size bytes starting from @param blob
   * represents a valid PartitionDirectoryKey.
   */
  static bool valid(const void* blob, size_t size) {
    return size == sizeof(PartitionDirectoryKey) &&
        reinterpret_cast<const char*>(blob)[0] == HEADER;
  }

  static constexpr char HEADER = prefix(KeyPrefix::PartitionDirectory);

 private:
  char header = HEADER;
  uint64_t log_id_big_endian_;
  uint64_t lsn_big_endian_;
  uint64_t partition_id_big_endian_;
} __attribute__((__packed__));

/**
 * Key for partition metadata. Contains metadata type and partition ID.
 */
class PartitionMetaKey {
 public:
  explicit PartitionMetaKey(PartitionMetadataType type,
                            partition_id_t partition_id)
      : header_(getHeader(type)),
        partition_id_big_endian_(htobe64(int64_t(partition_id))) {}

  // returns a key prefix for the given PartitionMetadataType
  static char getHeader(PartitionMetadataType type) {
    switch (type) {
      case PartitionMetadataType::STARTING_TIMESTAMP:
        return prefix(KeyPrefix::PartitionMeta_StartingTimestamp);
      case PartitionMetadataType::MIN_TIMESTAMP:
        return prefix(KeyPrefix::PartitionMeta_MinTimestamp);
      case PartitionMetadataType::MAX_TIMESTAMP:
        return prefix(KeyPrefix::PartitionMeta_MaxTimestamp);
      case PartitionMetadataType::LAST_COMPACTION:
        return prefix(KeyPrefix::PartitionMeta_LastCompaction);
      case PartitionMetadataType::COMPACTED_RETENTION:
        return prefix(KeyPrefix::PartitionMeta_CompactedRetention);
      case PartitionMetadataType::DIRTY:
        return prefix(KeyPrefix::PartitionMeta_Dirty);
      case PartitionMetadataType::MAX:
        break;
    }
    ld_check(false);
    return 0;
  }

  /**
   * Extracts the partition id from a memory segment that is a
   * PartitionMetaKey.
   */
  static partition_id_t getPartition(const void* blob) {
    const size_t offset = offsetof(PartitionMetaKey, partition_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    memcpy(&raw_be, ptr, sizeof raw_be);
    return be64toh(raw_be);
  }

  /**
   * Checks if a memory segment of @param size bytes starting from @param blob
   * represents a valid PartitionMetaKey.
   */
  static bool valid(const void* blob, size_t size, PartitionMetadataType type) {
    return size == sizeof(PartitionMetaKey) &&
        reinterpret_cast<const char*>(blob)[0] == getHeader(type);
  }

 private:
  char header_;
  uint64_t partition_id_big_endian_;
} __attribute__((__packed__));

class CopySetIndexKey {
 public:
  CopySetIndexKey(logid_t log_id, lsn_t lsn, char entry_type) {
    log_id_big_endian_ = htobe64(log_id.val_);
    lsn_big_endian_ = htobe64(lsn);
    ld_check(entry_type == SEEK_ENTRY_TYPE || entry_type == LAST_ENTRY_TYPE ||
             entry_type == BLOCK_ENTRY_TYPE || entry_type == SINGLE_ENTRY_TYPE);
    entry_type_ = entry_type;
  }

  static bool valid(const void* blob, size_t size) {
    if (size != sizeof(CopySetIndexKey)) {
      return false;
    }
    const char entry_type = *(reinterpret_cast<const char*>(blob) +
                              offsetof(CopySetIndexKey, entry_type_));
    return reinterpret_cast<const char*>(blob)[0] == HEADER &&
        (entry_type == SINGLE_ENTRY_TYPE || entry_type == BLOCK_ENTRY_TYPE);
  }

  static logid_t getLogID(const void* blob) {
    const size_t offset = offsetof(CopySetIndexKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    // Use memcpy (instead of casting to uint64_t*) to avoid undefined
    // behaviour due to unaligned access.  GCC and Clang optimize the
    // fixed-size memcpy to a mov on x64.
    uint64_t raw_be;
    static_assert(sizeof(logid_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return logid_t(int64_t(be64toh(raw_be)));
  }

  /**
   * Extracts the LSN from a memory segment that is a CopySetIndexKey
   */
  static lsn_t getLSN(const void* blob) {
    const size_t offset = offsetof(CopySetIndexKey, lsn_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(lsn_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return lsn_t(be64toh(raw_be));
  }

  // Returns true if this is a block entry
  static bool isBlockEntry(const void* blob) {
    const size_t offset = offsetof(CopySetIndexKey, entry_type_);
    const char entry_type = *(reinterpret_cast<const char*>(blob) + offset);
    // This shouldn't be called on SEEK_/LAST_ENTRY_TYPE entries
    ld_check(entry_type == BLOCK_ENTRY_TYPE || entry_type == SINGLE_ENTRY_TYPE);
    return entry_type == BLOCK_ENTRY_TYPE;
  }
  static constexpr char HEADER = prefix(KeyPrefix::CopySetIndex);
  static constexpr char SEEK_ENTRY_TYPE = 0; // used for seeking to the first
                                             // entry with that log_id and lsn
  static constexpr char LAST_ENTRY_TYPE = 0xFF; // used for setting the
                                                // upper bound for an iterator
  // TODO: (t9002309): it might be important to encounter single entries before
  // block records for the same LSN, as single records override block records.
  // So it could be important that SINGLE_ENTRY_TYPE < BLOCK_ENTRY_TYPE
  static constexpr char SINGLE_ENTRY_TYPE = 'S';
  static constexpr char BLOCK_ENTRY_TYPE = 'b';

 private:
  char header = HEADER;
  uint64_t log_id_big_endian_;
  uint64_t lsn_big_endian_;
  char entry_type_;
} __attribute__((__packed__));

// Format: 'I', log_id, index_type, key, LSN
namespace IndexKey {

constexpr char HEADER = prefix(KeyPrefix::Index);

inline folly::small_vector<char, 26> create(logid_t log_id,
                                            char index_type,
                                            std::string custom_key,
                                            lsn_t lsn) {
  int length = sizeof(char) + sizeof(uint64_t) + sizeof(char) +
      custom_key.size() + sizeof(uint64_t);
  if (index_type == FIND_KEY_INDEX) {
    length += sizeof(uint16_t);
  }
  folly::small_vector<char, 26> key(length);

  key[0] = HEADER;

  uint64_t log_id_big_endian = htobe64(log_id.val_);
  int offset = sizeof(char);
  memcpy(&key[offset], &log_id_big_endian, sizeof(log_id_big_endian));

  offset += sizeof(uint64_t);
  key[offset] = index_type;

  offset += sizeof(char);
  switch (index_type) {
    case FIND_KEY_INDEX:
      ld_check(custom_key.size() <= USHRT_MAX);
      uint16_t key_length;
      key_length = custom_key.size();

      memcpy(&key[offset], &key_length, sizeof(uint16_t));
      offset += sizeof(uint16_t);
      FOLLY_FALLTHROUGH;
    case FIND_TIME_INDEX:
      memcpy(&key[offset], custom_key.data(), custom_key.size());
      offset += custom_key.size();
      break;
  }

  uint64_t lsn_big_endian = htobe64(lsn);
  memcpy(&key[offset], &lsn_big_endian, sizeof(lsn_big_endian));

  offset += sizeof(uint64_t);
  ld_check(offset == key.size());
  return key;
}

/**
 * Extracts the log ID from a memory segment that is an IndexKey.
 */
inline logid_t getLogID(const void* blob) {
  const size_t offset = sizeof(HEADER);
  const void* ptr = reinterpret_cast<const char*>(blob) + offset;
  uint64_t raw_be;
  static_assert(sizeof(logid_t) == sizeof raw_be, "");
  memcpy(&raw_be, ptr, sizeof raw_be);
  return logid_t(be64toh(raw_be));
}

/**
 * Extracts the index type from a memory segment that is an IndexKey.
 */
inline char getIndexType(const void* blob) {
  const size_t offset = sizeof(HEADER) + sizeof(uint64_t);
  const void* ptr = reinterpret_cast<const char*>(blob) + offset;
  return *(reinterpret_cast<const char*>(ptr));
}

/**
 * Extracts the custom key from a memory segment that is an IndexKey.
 */
inline std::string getCustomKey(const void* blob) {
  char index_type = getIndexType(blob);
  size_t offset = sizeof(HEADER) + sizeof(uint64_t) + sizeof(char);
  const void* ptr = reinterpret_cast<const char*>(blob) + offset;
  std::string key;

  switch (index_type) {
    case FIND_TIME_INDEX:
      key.resize(FIND_TIME_KEY_SIZE);
      memcpy(&key[0], ptr, FIND_TIME_KEY_SIZE);
      return key;
    case FIND_KEY_INDEX:
      uint16_t key_length;
      memcpy(&key_length, ptr, sizeof(uint16_t));

      offset += sizeof(uint16_t);
      ptr = reinterpret_cast<const char*>(blob) + offset;
      key.resize(key_length);
      memcpy(&key[0], ptr, key_length);
      return key;
    default:
      ld_check(false);
      return nullptr; // can't get here
  }
}

/**
 * Extracts the LSN from a memory segment that is an IndexKey.
 */
inline lsn_t getLSN(const void* blob, size_t size) {
  const size_t lsn_offset = size - sizeof(lsn_t);
  const void* ptr = reinterpret_cast<const char*>(blob) + lsn_offset;

  uint64_t raw_be;
  static_assert(sizeof(lsn_t) == sizeof raw_be, "");
  memcpy(&raw_be, ptr, sizeof raw_be);

  return be64toh(raw_be);
}

/**
 * Checks if a memory segment of @param size bytes starting from @param blob
 * represents a valid IndexKey.
 */
inline bool valid(const void* blob, size_t size) {
  if (reinterpret_cast<const char*>(blob)[0] != HEADER) {
    return false;
  }

  size_t pre_key_size = sizeof(HEADER) + sizeof(uint64_t) + sizeof(char);
  size_t key_size = 0;
  size_t post_key_size = sizeof(uint64_t);
  char index_type = getIndexType(blob);
  switch (index_type) {
    case FIND_TIME_INDEX:
      key_size = FIND_TIME_KEY_SIZE;
      break;
    case FIND_KEY_INDEX:
      if (size < pre_key_size + sizeof(uint16_t) + post_key_size) {
        return false;
      }
      const void* ptr;
      ptr = reinterpret_cast<const char*>(blob) + pre_key_size;
      uint16_t key_length;
      memcpy(&key_length, ptr, sizeof(uint16_t));
      key_size = sizeof(uint16_t) + key_length;
      break;
    default:
      return false;
  }

  return size == pre_key_size + key_size + post_key_size;
}
} // namespace IndexKey

/**
 * A CustomIndexDirectoryKey is written to the metadata column family in RocksDB
 * and stores the mapping (log_id, index_type, partition_id) -> (min key, LSN).
 * This index is used for the findKey client operation which finds the LSN
 * of the record corresponding to a key. The index allows the correct partition
 * to be identified through a binary search on the minimum key.
 */
class CustomIndexDirectoryKey {
 public:
  CustomIndexDirectoryKey(logid_t log_id,
                          char index_type,
                          partition_id_t partition_id) {
    log_id_big_endian_ = htobe64(log_id.val_);
    index_type_ = index_type;
    partition_id_big_endian_ = htobe64(partition_id);
  }

  /**
   * Extracts the log ID from a memory segment that is a
   * CustomIndexDirectoryKey.
   */
  static logid_t getLogID(const void* blob) {
    const size_t offset = offsetof(CustomIndexDirectoryKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(logid_t) == sizeof raw_be, "");
    memcpy(&raw_be, ptr, sizeof raw_be);
    return logid_t(be64toh(raw_be));
  }

  /**
   * Extracts the index type from a memory segment that is a
   * CustomIndexDirectoryKey.
   */
  static char getIndexType(const void* blob) {
    const size_t offset = offsetof(CustomIndexDirectoryKey, index_type_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    return *(reinterpret_cast<const char*>(ptr));
  }

  /**
   * Extracts the partition id from a memory segment that is a
   * CustomIndexDirectoryKey.
   */
  static partition_id_t getPartition(const void* blob) {
    const size_t offset =
        offsetof(CustomIndexDirectoryKey, partition_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    memcpy(&raw_be, ptr, sizeof raw_be);
    return be64toh(raw_be);
  }

  /**
   * Checks if a memory segment of @param size bytes starting from @param blob
   * represents a valid CustomIndexDirectoryKey.
   */
  static bool valid(const void* blob, size_t size) {
    if (size != sizeof(CustomIndexDirectoryKey) ||
        reinterpret_cast<const char*>(blob)[0] != HEADER) {
      return false;
    }

    char index_type = getIndexType(blob);
    switch (index_type) {
      case FIND_KEY_INDEX:
        return true;
      default:
        return false;
    }
  }

  static constexpr std::array<char, 1> allEligibleIndexTypes() {
    return {{FIND_KEY_INDEX}};
  }

  static constexpr char HEADER = prefix(KeyPrefix::CustomIndexDirectory);

 private:
  char header = HEADER;
  uint64_t log_id_big_endian_;
  char index_type_;
  uint64_t partition_id_big_endian_;
} __attribute__((__packed__));

class LogSnapshotBlobKey {
 public:
  explicit LogSnapshotBlobKey(LocalLogStore::LogSnapshotBlobType type,
                              logid_t log_id)
      : header_(getHeader(type)), log_id_big_endian_(htobe64(log_id.val_)) {}

  // returns a key prefix for the given LogSnapshotBlobType
  static char getHeader(LocalLogStore::LogSnapshotBlobType type) {
    switch (type) {
      case LocalLogStore::LogSnapshotBlobType::RECORD_CACHE:
        return prefix(KeyPrefix::LogSnapshotBlob);
      case LocalLogStore::LogSnapshotBlobType::MAX:
        break;
    }
    ld_check(false);
    return 0;
  }

  // Extracts the log ID from a memory segment that is a LogSnapshotBlobKey
  static logid_t getLogID(const void* blob) {
    const auto offset = offsetof(LogSnapshotBlobKey, log_id_big_endian_);
    const void* ptr = reinterpret_cast<const char*>(blob) + offset;
    uint64_t raw_be;
    static_assert(sizeof(logid_t::raw_type) == sizeof(raw_be), "");
    memcpy(&raw_be, ptr, sizeof(raw_be));
    return logid_t(be64toh(raw_be));
  }

  // Checks if a memory segment represents a valid LogSnapshotBlobKey of a
  // given type.
  static bool valid(LocalLogStore::LogSnapshotBlobType type,
                    const void* blob,
                    size_t size) {
    return size == sizeof(LogSnapshotBlobKey) &&
        reinterpret_cast<const char*>(blob)[0] == getHeader(type);
  }

 private:
  char header_;
  uint64_t log_id_big_endian_;
} __attribute__((__packed__));

}}} // namespace facebook::logdevice::RocksDBKeyFormat

#ifdef __clang__
#pragma clang diagnostic pop
#endif
