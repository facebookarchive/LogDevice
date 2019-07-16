/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <type_traits>

#include <folly/Demangle.h>
#include <folly/Memory.h>
#include <folly/Portability.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

// Each record starts with a version number.
using event_log_record_version_t = uint32_t;

// record versions are independent. When changing the format of a record header,
// one needs to:
// - define a new version for that record,
// - pass it as argument when instantiating the FixedEventLogRecord template.
// - override fromPayload() to support deserializing older records
const event_log_record_version_t INITIAL_VERSION = 1;
const event_log_record_version_t SHARD_IS_REBUILT_FLAGS_VERSION = 2;

enum class EventType : uint8_t {
  INVALID = 0,
  SHARD_NEEDS_REBUILD,
  SHARD_IS_REBUILT,
  SHARD_ACK_REBUILT,
  SHARD_DONOR_PROGRESS,
  SHARD_ABORT_REBUILD,
  SHARD_UNDRAIN,
  SHARD_UNRECOVERABLE,
  MAX
};

std::string toString(const EventType&);

class EventLogRecord {
 public:
  static int fromPayload(Payload payload, std::unique_ptr<EventLogRecord>& out);
  virtual EventType getType() const = 0;
  /**
   * Serialize the record into a buffer.
   *
   * @param payload Buffer to which the record must be serialized. If nullptr,
   *                this function simply returns the actual size of the record
   *                when serialized and the `size` parameter is ignored.
   * @param size    Size of the buffer.
   * @return        actual size of the payload or -1 if the buffer is too small
   *                (and err is set to E::NOBUFS).
   */
  virtual int toPayload(void* payload, size_t size) const = 0;
  virtual std::string describe() const = 0;
  virtual ~EventLogRecord() {}
  explicit EventLogRecord(event_log_record_version_t version)
      : version_(version) {}
  event_log_record_version_t getVersion() const {
    return version_;
  }

 protected:
  int writeHeader(void* payload, size_t size) const;
  event_log_record_version_t version_{INITIAL_VERSION};
};

/**
 * A fixed size record in the event log.
 */
template <class Header,
          EventType Type,
          event_log_record_version_t FormatVersion = INITIAL_VERSION>
class FixedEventLogRecord : public EventLogRecord {
 public:
  explicit FixedEventLogRecord(
      Header header,
      event_log_record_version_t version = FormatVersion)
      : EventLogRecord(version), header(header) {}
  // Useful constructor that removes the need to construct a Header object.
  template <typename... Args>
  explicit FixedEventLogRecord(Args&&... args)
      : FixedEventLogRecord(Header{std::forward<Args>(args)...}) {}

  EventType getType() const override {
    return Type;
  }
  static int fromPayload(event_log_record_version_t version,
                         Payload payload,
                         std::unique_ptr<EventLogRecord>& out);
  int toPayload(void* payload, size_t size) const override;
  bool operator==(
      const FixedEventLogRecord<Header, Type, FormatVersion>& other) const;
  std::string describe() const override {
    return header.describe();
  }

  Header header;
};

/**
 * SHARD_NEEDS_REBUILD:
 * Written by external remediation scripts when they detect that shard
 * `shardIdx` of node `nodeIdx` needs to be rebuilt.
 */

struct SHARD_NEEDS_REBUILD_Header {
  SHARD_NEEDS_REBUILD_Header() {}
  SHARD_NEEDS_REBUILD_Header(node_index_t node_idx,
                             uint32_t shard_idx,
                             const std::string& source_str = "",
                             const std::string& details_str = "",
                             SHARD_NEEDS_REBUILD_flags_t flags_ = 0,
                             lsn_t conditional_version_ = LSN_INVALID)
      : nodeIdx(node_idx),
        shardIdx(shard_idx),
        flags(flags_),
        conditional_version(conditional_version_) {
    ld_check(source_str.length() < MAX_SOURCE_SZ);
    ld_check(details_str.length() < MAX_DETAILS_SZ);
    folly::strlcpy(source, source_str.c_str(), MAX_SOURCE_SZ);
    folly::strlcpy(details, details_str.c_str(), MAX_DETAILS_SZ);
  }

  node_index_t nodeIdx;
  uint32_t shardIdx;

  // Right now these sizes are fixed, but `source_size` and `details_size` are
  // populated in case we ever want to make these strings variable length.
  static constexpr size_t MAX_SOURCE_SZ = 64;
  static constexpr size_t MAX_DETAILS_SZ = 256;

  uint32_t source_size = MAX_SOURCE_SZ; // for forward compatibility
  char source[MAX_SOURCE_SZ];
  uint32_t details_size = MAX_DETAILS_SZ; // for forward compatibility
  char details[MAX_DETAILS_SZ];

  SHARD_NEEDS_REBUILD_flags_t flags{0};

  uint32_t time_range_data_offset = 0;
  uint32_t time_range_data_len = 0;

  // If CONDITIONAL_ON_VERSION flag is set,
  // only apply the delta if the current rebuilding version matches.
  lsn_t conditional_version = LSN_INVALID;

  // Backward/Forward compat
  static size_t MIN_SIZE() {
    return offsetof(SHARD_NEEDS_REBUILD_Header, time_range_data_offset);
  }

  // Flags.

  // Allow this node to also be a donor. This option should be used when this
  // node is expected to still have data. The node must remain up and running
  // during the rebuilding process otherwise rebuilding will get stuck.
  static constexpr SHARD_NEEDS_REBUILD_flags_t RELOCATE = (unsigned)1 << 0;

  // Rebuild this shard unconditionally until there is a SHARD_UNDRAIN message.
  static constexpr SHARD_NEEDS_REBUILD_flags_t DRAIN = (unsigned)1 << 1;

  // This record includes per-data class, time range information.
  static constexpr SHARD_NEEDS_REBUILD_flags_t TIME_RANGED = (unsigned)1 << 2;

  // If the shard is already AUTHORITATIVE_EMPTY, or if the shard is already
  // being rebuilt with the same flags, restart rebuilding regardless.
  static constexpr SHARD_NEEDS_REBUILD_flags_t FORCE_RESTART = (unsigned)1 << 3;

  // @see conditional_version.
  static constexpr SHARD_NEEDS_REBUILD_flags_t CONDITIONAL_ON_VERSION =
      (unsigned)1 << 4;

  // When adding new flags, don't forget to add them to describe().

  std::string getSource() const {
    return safe_print(source, MAX_SOURCE_SZ);
  }
  std::string getDetails() const {
    return safe_print(details, MAX_SOURCE_SZ);
  }

  std::string describe() const;
} __attribute__((__packed__));

class SHARD_NEEDS_REBUILD_Event : public EventLogRecord {
 public:
  explicit SHARD_NEEDS_REBUILD_Event(
      SHARD_NEEDS_REBUILD_Header h,
      RebuildingRangesMetadata* rrm = nullptr,
      event_log_record_version_t version = INITIAL_VERSION)
      : EventLogRecord(version), header(h) {
    if (rrm != nullptr && !rrm->empty()) {
      header.flags |= SHARD_NEEDS_REBUILD_Header::TIME_RANGED;
      time_ranges = *rrm;
    }
  }
  // Useful constructor that removes the need to construct a Header object.
  template <typename... Args>
  explicit SHARD_NEEDS_REBUILD_Event(Args&&... args)
      : SHARD_NEEDS_REBUILD_Event(
            SHARD_NEEDS_REBUILD_Header{std::forward<Args>(args)...}) {}

  EventType getType() const override {
    return EventType::SHARD_NEEDS_REBUILD;
  }
  static int fromPayload(event_log_record_version_t version,
                         Payload payload,
                         std::unique_ptr<EventLogRecord>& out);
  int toPayload(void* payload, size_t size) const override;
  bool operator==(const SHARD_NEEDS_REBUILD_Event& other) const;
  std::string describe() const override {
    return header.describe() +
        (time_ranges.empty() ? "" : toString(time_ranges));
  }

  SHARD_NEEDS_REBUILD_Header header;

  RebuildingRangesMetadata time_ranges;
};

/**
 * SHARD_IS_REBUILT:
 * Written by a storage node `donorNodeIdx` when it finished rebuilding the
 * data it had for shard `shardIdx`.
 * `version` is the LSN of the last SHARD_NEEDS_REBUILD event log record that
 * was taken into account.
 * `NON_AUTHORITATIVE` flag is set if the donor found some logs for which it
 * knows rebuilding could have missed records because these records could have
 * been fully replicated on the rebuilding set.
 */

struct SHARD_IS_REBUILT_Header {
  node_index_t donorNodeIdx;
  uint32_t shardIdx;
  lsn_t version;

  static const SHARD_IS_REBUILT_flags_t NON_AUTHORITATIVE = 1 << 1;
  SHARD_IS_REBUILT_flags_t flags;

  std::string describe() const;
} __attribute__((__packed__));

using SHARD_IS_REBUILT_Event =
    FixedEventLogRecord<SHARD_IS_REBUILT_Header,
                        EventType::SHARD_IS_REBUILT,
                        SHARD_IS_REBUILT_FLAGS_VERSION>;

template <>
int SHARD_IS_REBUILT_Event::fromPayload(event_log_record_version_t version,
                                        Payload payload,
                                        std::unique_ptr<EventLogRecord>& out);

/**
 * SHARD_ACK_REBUILT:
 * Written by node `nodeIdx` when it acknowledged that all nodes in the
 * cluster rebuilt its shard `shardIdx`.
 * `version` is the LSN of the last SHARD_NEEDS_REBUILD event log record that
 * was taken into account.
 */

struct SHARD_ACK_REBUILT_Header {
  node_index_t nodeIdx;
  uint32_t shardIdx;
  lsn_t version;
  std::string describe() const;
} __attribute__((__packed__));

using SHARD_ACK_REBUILT_Event =
    FixedEventLogRecord<SHARD_ACK_REBUILT_Header, EventType::SHARD_ACK_REBUILT>;

/**
 * SHARD_DONOR_PROGRESS:
 * Written by node `donorNodeIdx` when it slides its local timestamp window for
 * shard `shardIdx`. `nextTimestamp` is the new low end of its local window.
 */

struct SHARD_DONOR_PROGRESS_Header {
  node_index_t donorNodeIdx;
  uint32_t shardIdx;
  int64_t nextTimestamp;
  lsn_t version;
  std::string describe() const;
} __attribute__((__packed__));

using SHARD_DONOR_PROGRESS_Event =
    FixedEventLogRecord<SHARD_DONOR_PROGRESS_Header,
                        EventType::SHARD_DONOR_PROGRESS>;

/**
 * SHARD_UNDRAIN
 * Used to indicate that a storage node that has been drained can now
 * acknowledge rebuilding and become FULLY_AUTHORITATIVE. Also used to cancel a
 * drain.
 */

struct SHARD_UNDRAIN_Header {
  node_index_t nodeIdx;
  uint32_t shardIdx;
  std::string describe() const;
} __attribute__((__packed__));

using SHARD_UNDRAIN_Event =
    FixedEventLogRecord<SHARD_UNDRAIN_Header, EventType::SHARD_UNDRAIN>;

/**
 * SHARD_ABORT_REBUILD
 * Abort rebuilding of the given shard. Acts as if all donors completed
 * rebuilding the shard and the shard sent SHARD_ACK_REBUILT.
 * If version != LSN_INVALID, the message is discarded if the rebuilding version
 * when received doesn't match, which means rebuilding was restarted between the
 * moment the sender of this message intended to append it and the moment the
 * message was really appended.
 */

struct SHARD_ABORT_REBUILD_Header {
  node_index_t nodeIdx;
  uint32_t shardIdx;
  lsn_t version;
  std::string describe() const;
} __attribute__((__packed__));

using SHARD_ABORT_REBUILD_Event =
    FixedEventLogRecord<SHARD_ABORT_REBUILD_Header,
                        EventType::SHARD_ABORT_REBUILD>;

/**
 * SHARD_UNRECOVERABLE
 * Used to indicate that the data on a shard was definitely lost and there is no
 * chance to recover it. Readers should not stall hoping this data may be
 * recovered.
 *
 * The shard is marked back to recoverable when we receive a SHARD_ACK_REBUILT,
 * or SHARD_ABORT_REBUILD event for it, or when switching from time-ranged to
 * full rebuilding.
 */

struct SHARD_UNRECOVERABLE_Header {
  node_index_t nodeIdx;
  uint32_t shardIdx;
  std::string describe() const;
} __attribute__((__packed__));

using SHARD_UNRECOVERABLE_Event =
    FixedEventLogRecord<SHARD_UNRECOVERABLE_Header,
                        EventType::SHARD_UNRECOVERABLE>;
}} // namespace facebook::logdevice

#include "logdevice/common/event_log/EventLogRecord-inl.h"
