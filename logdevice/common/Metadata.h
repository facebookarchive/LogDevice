/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "logdevice/common/DataClass.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

/**
 * Macro for generating simple metadata serialization methods, used by most
 * classes in this file.
 */
#define GEN_METADATA_SERIALIZATION_METHODS(Klass, member, tostring) \
  Slice serialize() const override {                                \
    return Slice(&(member), sizeof(member));                        \
  }                                                                 \
  int deserialize(Slice blob) override {                            \
    if (blob.size != sizeof(member)) {                              \
      err = E::MALFORMED_RECORD;                                    \
      return -1;                                                    \
    }                                                               \
    std::memcpy(&member, blob.data, sizeof(member));                \
    return 0;                                                       \
  }                                                                 \
  std::string toString() const override {                           \
    return std::string(#Klass "(") + tostring + ")";                \
  }

namespace facebook { namespace logdevice {

/**
 * @file  Different types of metadata stored in the local log store for each
 *        log or shard.
 *
 *        The enum values must be consecutive, start at 0 and end at MAX-1.
 *
 *        This file uses virtual inline functions and the 'final' keyword to
 *        aid the compiler in function devirtualization. Perform static_casts
 *        to a specific metadata type in client code, when possible, to avoid
 *        unnecessary virtual function calls.
 */

enum class LogMetadataType : uint8_t {
#define LOG_METADATA_TYPE(name, value) name = (value),
#include "logdevice/common/log_metadata_type.inc"
};
EnumMap<LogMetadataType, std::string, LogMetadataType::MAX>&
logMetadataTypeNames();

enum class PerEpochLogMetadataType : uint8_t {
  RECOVERY = 0,
  MUTABLE = 1,

  MAX = 2
};
EnumMap<PerEpochLogMetadataType, std::string, PerEpochLogMetadataType::MAX>&
perEpochLogMetadataTypeNames();

enum class StoreMetadataType : uint8_t {
  CLUSTER_MARKER = 0,
  REBUILDING_COMPLETE = 1,
  // Value 2 is skipped for historical reasons. It's safe to reuse it.
  UNUSED = 2,
  REBUILDING_RANGES = 3,

  MAX = 4
};
EnumMap<StoreMetadataType, std::string, StoreMetadataType::MAX>&
storeMetadataTypeNames();

class Metadata {
 public:
  virtual ~Metadata() {}

  /**
   * @return  a representation of this Metadata object to be written to the
   *          log store (note that returned Slice's lifecycle is tied to this
   *          object's)
   */
  virtual Slice serialize() const = 0;

  /**
   * Restore metadata fields from the serialized representation read from the
   * log store.
   *
   * @return 0 on success, -1 with err set to E::MALFORMED_RECORD if the
   *         serialized representation is not valid
   */
  virtual int deserialize(Slice blob) = 0;

  /**
   * @return  if the metadata represents a valid metadata and is safe to
   *          write to persistent storage.
   */
  virtual bool valid() const {
    return true;
  }

  virtual std::string toString() const = 0;
};

class LogMetadata : public Metadata {
 public:
  /**
   * @return  the type of metadata this class represents.
   */
  virtual LogMetadataType getType() const = 0;
};

class PerEpochLogMetadata : public Metadata {
 public:
  /**
   * @return  the type of metadata this class represents.
   */
  virtual PerEpochLogMetadataType getType() const = 0;

  /**
   * @return  if the metadata is a valid one but also considered `empty'.
   *          LocalLogStore may choose not to store it or erase the existing
   *          one on update operations
   */
  virtual bool empty() const {
    return false;
  }

  /**
   * @return  if the metadata is preempted by a Seal
   *
   */
  virtual bool isPreempted(Seal /*seal*/) const {
    return false;
  }

  /**
   * reset the metadata to invalid state
   */
  virtual void reset() = 0;

  /**
   * @param  new_metadata    in/out parameter of a PerEpochLogMetadata.
   *
   * @return   status of the update, can be:
   *           E::OK         update successful, output metadata will be
   *                         the updated value
   *           E::UPTODATE   there is no need to update, output metadata is
   *                         written with the existing value that prevents
   *                         the update
   */
  virtual Status update(PerEpochLogMetadata& new_metadata) = 0;
};

class StoreMetadata : public Metadata {
 public:
  /**
   * @return  the type of metadata this class represents.
   */
  virtual StoreMetadataType getType() const = 0;
};

/**
 * Metadata entry that supports comparison. Used by LocalLogStore's
 * updateLogMetadata method.
 */
class ComparableLogMetadata : public LogMetadata {
 public:
  virtual bool operator<(const ComparableLogMetadata& rhs) const = 0;
};

class LastReleasedMetadata final : public LogMetadata {
 public:
  LastReleasedMetadata() = default;
  explicit LastReleasedMetadata(lsn_t lsn) : last_released_lsn_(lsn) {}

  LogMetadataType getType() const override {
    return LogMetadataType::LAST_RELEASED;
  }

  // must write a valid epoch or not set
  bool valid() const override {
    return epoch_valid_or_unset(lsn_to_epoch(last_released_lsn_));
  }

  GEN_METADATA_SERIALIZATION_METHODS(LastReleasedMetadata,
                                     last_released_lsn_,
                                     lsn_to_string(last_released_lsn_))

  lsn_t last_released_lsn_;
};

class TrimMetadata final : public ComparableLogMetadata {
 public:
  TrimMetadata() = default;
  explicit TrimMetadata(lsn_t lsn) : trim_point_(lsn) {}

  LogMetadataType getType() const override {
    return LogMetadataType::TRIM_POINT;
  }

  // must write a valid epoch or not set
  bool valid() const override {
    return epoch_valid_or_unset(lsn_to_epoch(trim_point_));
  }

  GEN_METADATA_SERIALIZATION_METHODS(TrimMetadata,
                                     trim_point_,
                                     lsn_to_string(trim_point_))

  bool operator<(const ComparableLogMetadata& rhs) const override {
    ld_assert(dynamic_cast<const TrimMetadata*>(&rhs) != nullptr);
    const TrimMetadata* meta = static_cast<const TrimMetadata*>(&rhs);
    return trim_point_ < meta->trim_point_;
  }

  lsn_t trim_point_;
};

class SealMetadata : public ComparableLogMetadata {
 public:
  SealMetadata() = default;
  explicit SealMetadata(Seal seal) : seal_(seal) {}

  // must write a valid Seal
  bool valid() const override {
    return seal_.valid();
  }

  LogMetadataType getType() const override {
    return LogMetadataType::SEAL;
  }

  GEN_METADATA_SERIALIZATION_METHODS(SealMetadata, seal_, seal_.toString())

  bool operator<(const ComparableLogMetadata& rhs) const override {
    ld_assert(dynamic_cast<const SealMetadata*>(&rhs) != nullptr);
    const SealMetadata* meta = static_cast<const SealMetadata*>(&rhs);
    return seal_ < meta->seal_;
  }

  Seal seal_;
};

/**
 * A separate seal metadata for storing soft seals
 */
class SoftSealMetadata final : public SealMetadata {
 public:
  using SealMetadata::SealMetadata;
  LogMetadataType getType() const override {
    return LogMetadataType::SOFT_SEAL;
  }
  std::string toString() const override {
    return "SoftSealMetadata(" + seal_.toString() + ")";
  }
};

/**
 * For logs that get removed from the config, maintain a timestamp
 * of when they can get trimmed(see rocksdb setting:
 * log_trimming_grace_period_).
 * Without this, frequent node restarts can delay trimming forever.
 */
class LogRemovalTimeMetadata final : public LogMetadata {
 public:
  LogRemovalTimeMetadata() = default;
  explicit LogRemovalTimeMetadata(std::chrono::seconds t)
      : log_removal_time_(t) {}

  LogMetadataType getType() const override {
    return LogMetadataType::LOG_REMOVAL_TIME;
  }

  GEN_METADATA_SERIALIZATION_METHODS(LogRemovalTimeMetadata,
                                     log_removal_time_,
                                     std::to_string(log_removal_time_.count()))

  std::chrono::seconds log_removal_time_{std::chrono::seconds::max()};
};

/**
 * Contains the last epoch that the storage node knows is clean for a log.
 * Read and updated when RELEASE and CLEAN messages are received (see
 * PurgeCoordinator and PurgeUncleanEpochs).
 */
class LastCleanMetadata final : public LogMetadata {
 public:
  LastCleanMetadata() = default;
  explicit LastCleanMetadata(epoch_t epoch) : epoch_(epoch) {}

  LogMetadataType getType() const override {
    return LogMetadataType::LAST_CLEAN;
  }

  // must write a valid epoch or not set
  bool valid() const override {
    return epoch_valid_or_unset(epoch_);
  }

  GEN_METADATA_SERIALIZATION_METHODS(LastCleanMetadata,
                                     epoch_,
                                     "e" + std::to_string(epoch_.val_))

  epoch_t epoch_;
};

class LogMetadataFactory final {
 public:
  static std::unique_ptr<LogMetadata> create(LogMetadataType type);
};

/**
 * Present if no rebuilding is needed. Checking for this metadata is almost
 * equivalent to checking that all logs have RebuildingCheckpointMetadata with
 * LSN_MAX. The only difference is when new logs are added to config -
 * RebuildingCompleteMetadata remains valid, while checking for all
 * RebuildingCheckpointMetadata would erroneously say that rebuilding is needed.
 */
class RebuildingCompleteMetadata final : public StoreMetadata {
 public:
  RebuildingCompleteMetadata() = default;

  StoreMetadataType getType() const override {
    return StoreMetadataType::REBUILDING_COMPLETE;
  }

  Slice serialize() const override {
    return Slice(this, 0);
  }

  int deserialize(Slice blob) override {
    if (blob.size != 0) {
      err = E::MALFORMED_RECORD;
      return -1;
    }
    return 0;
  }

  std::string toString() const override {
    return "RebuildingCompleteMetadata";
  }
};

/**
 * Name of the cluster, node offset and shard index. If it doesn't match,
 * the data was probably misplaced, and server should refuse to start.
 * The marker is a string of the form
 * "<cluster_name>:N<node_offset>:shard<shard_index>",
 * e.g. logdevice.test.frc3a:N0:shard0
 */
class ClusterMarkerMetadata final : public StoreMetadata {
 public:
  ClusterMarkerMetadata() = default;
  explicit ClusterMarkerMetadata(std::string marker) : marker_(marker) {}

  StoreMetadataType getType() const override {
    return StoreMetadataType::CLUSTER_MARKER;
  }

  Slice serialize() const override {
    return Slice(marker_.data(), marker_.size());
  }

  int deserialize(Slice blob) override {
    marker_ = std::string(reinterpret_cast<const char*>(blob.data), blob.size);
    return 0;
  }

  std::string toString() const override {
    return "ClusterMarkerMetadata(" + marker_ + ")";
  }

  std::string marker_;
};

/**
 * Present and non-empty on a shard if LogDevice failed to shutdown cleanly
 * with unpersisted write data. The [Append/Rebuild/...] time ranges are
 * used to trigger a cluster rebuild to restore any data that might have
 * been lost.
 *
 * LogDevice tracks two types of dirty time range state: the time ranges of
 * in-flight data that will be lost if the current LogDevice instance fails
 * to shut down cleanly (PartitionDirtyMetadata), and the time ranges for
 * in-flight data that may have been lost due to one or more previous ungraceful
 * shutdowns of LogDevice (RebuildingRangesMetadata).
 *
 * During normal operation, the PartitionedRocksDBStore durably tracks time
 * partitions that contain in-flight, unpersisted data by modifying
 * PartitionDirtyMetadata records. At startup, dirty PartitionDirtyMetadata
 * records (records that should have been marked clean by our shutdown process)
 * are merged into a new or existing RebuildingRangesMetadata record. The
 * RebuildingRangesMetadata is retired once the time ranges are successfully
 * rebuilt and the replication factor has been restored.
 *
 * This separation of concerns simplifies both logic and terminology. For
 * example, calling a partition "dirty" refers to either it currently
 * containing unpersisted data, or, for a brief time during startup as
 * records are merged into a RebuildingRangesMetadata record, the
 * partition being left in a dirty state by the last instance of LogDevice.
 * The partition logic isn't responsible for preserving the long term
 * history of the partition's dirty state and can just focus on transitioning
 * a partition from dirty to clean as RocksDB MemTables are retired.
 *
 * DataClassHeader: APPEND
 *   TimeRange
 *   ...
 *   TimeRange
 */
class RebuildingRangesMetadata final : public StoreMetadata {
  struct Header {
    uint16_t len;
    uint8_t pad[2]{};
    uint32_t data_classes_offset;
    uint32_t data_classes_len;
  };
  static_assert(sizeof(Header) == 12,
                "RebuildingRangesMetadata::Header is not packed.");

  struct DataClassHeader {
    uint16_t len;
    DataClass data_class;
    uint8_t pad = 0;
    uint32_t time_ranges_offset;
    uint32_t time_ranges_len;
  };
  static_assert(sizeof(DataClassHeader) == 12,
                "RebuildingRangesMetadata::DataClassHeader is not packed.");

  struct TimeRange {
    uint16_t len;
    uint8_t pad[6]{};
    uint64_t start_ms;
    uint64_t end_ms;
  };
  static_assert(sizeof(TimeRange) == 24,
                "RebuildingRangesMetadata::TimeRange is not packed.");

  PerDataClassTimeRanges per_dc_dirty_ranges_;
  mutable std::vector<uint8_t> serialize_buffer_;

 public:
  RebuildingRangesMetadata() = default;

  StoreMetadataType getType() const override {
    return StoreMetadataType::REBUILDING_RANGES;
  }
  Slice serialize() const override;
  std::string toString() const override;
  int deserialize(Slice blob) override;

  const PerDataClassTimeRanges& getDCDirtyRanges() const {
    return per_dc_dirty_ranges_;
  }

  bool operator==(const RebuildingRangesMetadata& other) const {
    // Ignore the serialization buffer since serialization may
    // never have occurred or the ranges modified post serialization.
    return per_dc_dirty_ranges_ == other.per_dc_dirty_ranges_;
  }

  bool empty() const {
    return per_dc_dirty_ranges_.empty();
  }

  bool empty(DataClass dc) const {
    auto dcr_kv = per_dc_dirty_ranges_.find(dc);
    return dcr_kv == per_dc_dirty_ranges_.end() || dcr_kv->second.empty();
  }

  void addTimeInterval(DataClass dc, RecordTimeInterval time_range);
};

class StoreMetadataFactory final {
 public:
  static std::unique_ptr<StoreMetadata> create(StoreMetadataType type);
};

/**
 * Per-log checkpoint on a donor node for rebuilding.
 * The checkpoint contains the last LSN rebuilt by this donor node.
 * It also contains the rebuilding version, @see LogRebuilding::version_.
 */
class RebuildingCheckpointMetadata final : public LogMetadata {
 public:
  RebuildingCheckpointMetadata() = default;
  RebuildingCheckpointMetadata(lsn_t rebuilding_version, lsn_t rebuilt_upto)
      : data_{rebuilding_version, rebuilt_upto} {}

  LogMetadataType getType() const override {
    return LogMetadataType::REBUILDING_CHECKPOINT;
  }

  GEN_METADATA_SERIALIZATION_METHODS(
      RebuildingCheckpointMetadata,
      data_,
      "v " + lsn_to_string(data_.rebuilding_version) + " upto " +
          lsn_to_string(data_.rebuilt_upto))

  struct {
    lsn_t rebuilding_version;
    lsn_t rebuilt_upto;
  } data_ __attribute__((__packed__));
};

/**
 * Per-epoch log metadata stored by epoch recovery in the CLEAN phase.
 * Stores information about the EpochRecovery instance, including:
 * 1) epoch of the sequencer that initiated recovery;
 * 2) window of ESNs used in the EpochRecovery instance,
 *    which is (last_known_good, last_digest_esn].
 *
 * Also stores the accumulative byte offset of the epoch.
 */
class EpochRecoveryMetadata final : public PerEpochLogMetadata {
 public:
  using FlagsType = uint16_t;

  // default construction leaves the metadata in an invalid state
  explicit EpochRecoveryMetadata()
      : header_{EPOCH_INVALID,
                ESN_INVALID,
                ESN_INVALID,
                0,
                BYTE_OFFSET_INVALID,
                BYTE_OFFSET_INVALID} {}

  EpochRecoveryMetadata(epoch_t sequencer_epoch,
                        esn_t last_known_good,
                        esn_t last_digest_esn,
                        FlagsType flags,
                        uint64_t epoch_end_offset,
                        uint64_t epoch_size)
      : header_{sequencer_epoch,
                last_known_good,
                last_digest_esn,
                flags,
                epoch_end_offset,
                epoch_size} {}

  PerEpochLogMetadataType getType() const override {
    return PerEpochLogMetadataType::RECOVERY;
  }

  // check if the metadata is a valid one generated by epoch recovery
  bool valid() const override {
    return header_.sequencer_epoch != EPOCH_INVALID &&
        header_.last_digest_esn >= header_.last_known_good;
  }

  bool empty() const override {
    // return true if the metadata represents an empty epoch determined
    // by EpochRecovery
    return valid() && header_.last_digest_esn == ESN_INVALID;
  }

  bool isPreempted(Seal seal) const override {
    return seal.valid() &&
        (seal.epoch == EPOCH_MAX ||
         header_.sequencer_epoch.val_ < seal.epoch.val_ + 1);
  }

  GEN_METADATA_SERIALIZATION_METHODS(EpochRecoveryMetadata,
                                     header_,
                                     toStringShort())

  // TODO: describe the update rule
  Status update(PerEpochLogMetadata& new_metadata) override;

  bool operator==(const EpochRecoveryMetadata& rhs) const {
    return this->header_ == rhs.header_;
  }

  void reset() override {
    header_ = {EPOCH_INVALID,
               ESN_INVALID,
               ESN_INVALID,
               0,
               BYTE_OFFSET_INVALID,
               BYTE_OFFSET_INVALID};
  }

  // Return a human readable string for the content of the metadata.
  // toString() returns "EpochRecoveryMetadata(<toStringShort()>)".
  std::string toStringShort() const;

  struct Header {
    // epoch of the sequencer when the log recovery procedure was started
    epoch_t sequencer_epoch;

    // last known good esn of the epoch recovery
    esn_t last_known_good;

    // last esn of the digest collected by epoch recovery
    esn_t last_digest_esn;

    // flags for future compatibility
    FlagsType flags;

    uint64_t epoch_end_offset;

    // approximate epoch size in bytes
    uint64_t epoch_size;

    bool operator==(const Header& rhs) const {
      // need to compare flags before comparing rest of memory, because rhs
      // may have a different size if flags differ
      return (flags == rhs.flags) && !std::memcmp(this, &rhs, sizeof(*this));
    }
  } __attribute__((__packed__));

  static_assert(sizeof(Header) == 30,
                "EpochRecoveryMetadata::Header has changed or is unpacked");
  static_assert(std::is_standard_layout<Header>::value,
                "EpochRecoveryMetadata::Header has non-standard layout");

  // format of this metadata value in the local log store:
  // linear header followed by optional data specified by `flags' field
  Header header_;

 private:
  static size_t sizeInLinearBuffer(const Header& header) {
    return sizeof(header);
  }
};

/**
 * Per-epoch metadata that changes frequently, with every record that is
 * appended to the epoch.
 */
class MutablePerEpochLogMetadata final : public PerEpochLogMetadata {
 public:
  using FlagsType = uint16_t;

  struct Data {
    // flags for versioning and future compatibility
    FlagsType flags;

    // last known good esn of the epoch
    esn_t last_known_good;

    // epoch size in bytes
    uint64_t epoch_size;
  } __attribute__((__packed__));

  MutablePerEpochLogMetadata()
#ifdef DEBUG
      : data_{0xDEAD} {
  }
#else
      = default;
#endif

  MutablePerEpochLogMetadata(FlagsType flags,
                             esn_t last_known_good,
                             uint64_t epoch_size)
      : data_{flags, last_known_good, epoch_size} {}

  PerEpochLogMetadataType getType() const override {
    return PerEpochLogMetadataType::MUTABLE;
  }

  static bool valid(Slice blob) {
    return blob.size == sizeof(data_) &&
        static_cast<const Data*>(blob.data)->flags == 0;
  }

  bool valid() const override {
    return data_.flags == 0;
  }

  void reset() override {
    data_.flags = 0;
    data_.last_known_good = ESN_INVALID;
    data_.epoch_size = 0;
  }

  Status update(PerEpochLogMetadata&) override {
    return E::NOTSUPPORTED;
  }

  Slice serialize() const override {
    return Slice(&data_, sizeof(data_));
  }

  int deserialize(Slice blob) override {
    if (blob.size != sizeof(data_)) {
      err = E::MALFORMED_RECORD;
      return -1;
    }
    std::memcpy(&data_, blob.data, blob.size);
    return 0;
  }

  std::string toString() const override;

  void merge(const MutablePerEpochLogMetadata& other) {
    ld_check(valid() && other.valid());
    merge(other.data_);
  }

  void merge(const Slice& other) {
    ld_check(valid() && valid(other));
    merge(*static_cast<const Data*>(other.data));
  }

  Data data_;

 private:
  void merge(const Data& other) {
    // update last_known_good and epoch_size to maximum, respectively
    if (other.last_known_good > data_.last_known_good) {
      data_.last_known_good = other.last_known_good;
    }
    if (other.epoch_size != BYTE_OFFSET_INVALID &&
        (data_.epoch_size == BYTE_OFFSET_INVALID ||
         other.epoch_size > data_.epoch_size)) {
      data_.epoch_size = other.epoch_size;
    }
  }
};

class PerEpochLogMetadataFactory final {
 public:
  static std::unique_ptr<PerEpochLogMetadata>
  create(PerEpochLogMetadataType type);
};

}} // namespace facebook::logdevice
