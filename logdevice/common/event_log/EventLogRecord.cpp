/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogRecord.h"

#include <folly/Conv.h>
#include <folly/String.h>

namespace facebook { namespace logdevice {

std::string toString(const EventType& type) {
  switch (type) {
    case EventType::MAX:
    case EventType::INVALID:
      break;
    case EventType::SHARD_NEEDS_REBUILD:
      return "SHARD_NEEDS_REBUILD";
    case EventType::SHARD_IS_REBUILT:
      return "SHARD_IS_REBUILT";
    case EventType::SHARD_ACK_REBUILT:
      return "SHARD_ACK_REBUILT";
    case EventType::SHARD_DONOR_PROGRESS:
      return "SHARD_DONOR_PROGRESS";
    case EventType::SHARD_ABORT_REBUILD:
      return "SHARD_ABORT_REBUILD";
    case EventType::SHARD_UNDRAIN:
      return "SHARD_UNDRAIN";
    case EventType::SHARD_UNRECOVERABLE:
      return "SHARD_UNRECOVERABLE";
  }
  return "INVALID";
}

int EventLogRecord::fromPayload(Payload payload,
                                std::unique_ptr<EventLogRecord>& out) {
  // Each record is prefixed with a version number and the type of the message.
  const size_t min_expected_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  if (payload.size() < min_expected_size) {
    ld_error("Invalid record in event log: expecting at least %zu bytes but "
             "got %zu bytes",
             min_expected_size,
             payload.size());
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());

  event_log_record_version_t version;
  memcpy(&version, ptr, sizeof(version));
  ptr += sizeof(version);

  EventType type;
  memcpy(&type, ptr, sizeof(type));
  ptr += sizeof(type);

  if (type <= EventType::INVALID || type >= EventType::MAX) {
    ld_error(
        "Invalid record in event log: unexpected type %u", (unsigned int)type);
    err = E::BADMSG;
    return -1;
  }

  switch (type) {
    case EventType::INVALID:
    case EventType::MAX:
      break;
    case EventType::SHARD_NEEDS_REBUILD:
      return SHARD_NEEDS_REBUILD_Event::fromPayload(version, payload, out);
    case EventType::SHARD_IS_REBUILT:
      return SHARD_IS_REBUILT_Event::fromPayload(version, payload, out);
    case EventType::SHARD_ACK_REBUILT:
      return SHARD_ACK_REBUILT_Event::fromPayload(version, payload, out);
    case EventType::SHARD_DONOR_PROGRESS:
      return SHARD_DONOR_PROGRESS_Event::fromPayload(version, payload, out);
    case EventType::SHARD_ABORT_REBUILD:
      return SHARD_ABORT_REBUILD_Event::fromPayload(version, payload, out);
    case EventType::SHARD_UNDRAIN:
      return SHARD_UNDRAIN_Event::fromPayload(version, payload, out);
    case EventType::SHARD_UNRECOVERABLE:
      return SHARD_UNRECOVERABLE_Event::fromPayload(version, payload, out);
  }

  ld_check(false);
  err = E::INTERNAL;
  return -1;
}

int EventLogRecord::writeHeader(void* payload, size_t size) const {
  const size_t header_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  if (payload) {
    if (header_size > size) {
      err = E::NOBUFS;
      return -1;
    }

    memcpy(payload, &version_, sizeof(version_));
    payload = reinterpret_cast<char*>(payload) + sizeof(version_);
    const EventType type = getType();
    memcpy(payload, &type, sizeof(type));
  }

  return header_size;
}

// These flags are odr-used in tests and so must be defined in a compilation
// unit.  This requirement goes away in C++-17 for inline static data members.
// constexpr members are implicitly inline.
constexpr SHARD_NEEDS_REBUILD_flags_t SHARD_NEEDS_REBUILD_Header::RELOCATE;
constexpr SHARD_NEEDS_REBUILD_flags_t SHARD_NEEDS_REBUILD_Header::DRAIN;
constexpr SHARD_NEEDS_REBUILD_flags_t SHARD_NEEDS_REBUILD_Header::TIME_RANGED;
constexpr SHARD_NEEDS_REBUILD_flags_t SHARD_NEEDS_REBUILD_Header::FORCE_RESTART;
constexpr SHARD_NEEDS_REBUILD_flags_t
    SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;

std::string SHARD_NEEDS_REBUILD_Header::describe() const {
  std::vector<std::string> flags_str;
  SHARD_NEEDS_REBUILD_flags_t remaining_flags = flags;
#define FLAG(f)                                       \
  if (flags & SHARD_NEEDS_REBUILD_Header::f) {        \
    remaining_flags ^= SHARD_NEEDS_REBUILD_Header::f; \
    flags_str.push_back(#f);                          \
  }
  FLAG(RELOCATE);
  FLAG(DRAIN);
  FLAG(TIME_RANGED);
  FLAG(FORCE_RESTART);
  FLAG(CONDITIONAL_ON_VERSION);
#undef FLAG

  if (remaining_flags) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "A flag %u seems to be missing in "
                    "SHARD_NEEDS_REBUILD_Header::describe(). Please update the "
                    "code at " __FILE__ ":%d. It's easy, I promise.",
                    __LINE__,
                    remaining_flags);
    flags_str.push_back(std::to_string(remaining_flags));
  }

  std::string res = "SHARD_NEEDS_REBUILD(";
  res += "nodeIdx=" + folly::to<std::string>(nodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ", source=" + getSource();
  res += ", details=" + getDetails();
  res += ", flags=" + folly::join("|", flags_str);
  if (flags & SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION) {
    res += ", version=" + lsn_to_string(conditional_version);
  }
  res += ")";
  return res;
}

int SHARD_NEEDS_REBUILD_Event::fromPayload(
    event_log_record_version_t version,
    Payload payload,
    std::unique_ptr<EventLogRecord>& out) {
  const size_t header_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  const size_t min_expected_size = SHARD_NEEDS_REBUILD_Header::MIN_SIZE();
  if (payload.size() < header_size + min_expected_size) {
    ld_error("Invalid record in event log: expecting at least %zu bytes "
             "after the header",
             min_expected_size);
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  ptr += header_size;
  SHARD_NEEDS_REBUILD_Header header;
  memcpy(&header, ptr, std::min(sizeof(header), payload.size() - header_size));

  RebuildingRangesMetadata rrm;
  if (header.flags & header.TIME_RANGED) {
    // Offset is from beginning of SHARD_NEEDS_REBUILD_Header which
    // ptr is pointing to.
    size_t event_size = payload.size() - header_size;
    if (header.time_range_data_len > (event_size - sizeof(header)) ||
        header.time_range_data_offset < sizeof(header) ||
        header.time_range_data_offset >= event_size ||
        (event_size - header.time_range_data_offset) <
            header.time_range_data_len) {
      ld_error("SHARD_NEEDS_REBUILD: Invalid time range data pointers");
      err = E::BADMSG;
      return -1;
    }

    Slice time_range_data(
        ptr + header.time_range_data_offset, header.time_range_data_len);
    if (rrm.deserialize(time_range_data) != 0) {
      ld_error("SHARD_NEEDS_REBUILD: Unable to deserialize time range data.");
      err = E::BADMSG;
      return -1;
    }
  }

  if (header.flags & header.CONDITIONAL_ON_VERSION) {
    if (header.conditional_version == LSN_INVALID) {
      ld_error("SHARD_NEEDS_REBUILD: CONDITIONAL_ON_VERSION flag is set but "
               "the provided version is equal to LSN_INVALID");
      err = E::BADMSG;
      return -1;
    }
  } else {
    // May containt "garbage" due to time range data starting where this field
    // is in the header for a newer client.
    header.conditional_version = LSN_INVALID;
  }

  auto record =
      std::make_unique<SHARD_NEEDS_REBUILD_Event>(header, &rrm, version);

  out = std::move(record);
  return 0;
}

int SHARD_NEEDS_REBUILD_Event::toPayload(void* payload, size_t size) const {
  const int header_size = writeHeader(payload, size);
  if (header_size < 0) {
    return -1;
  }

  SHARD_NEEDS_REBUILD_Header h = header;
  Slice rrm = time_ranges.serialize();
  if (!time_ranges.empty()) {
    h.time_range_data_offset = sizeof(header);
    h.time_range_data_len = rrm.size;
    h.flags |= header.TIME_RANGED;
  } else {
    h.time_range_data_offset = 0;
    h.time_range_data_len = 0;
    h.flags &= ~header.TIME_RANGED;
    rrm.size = 0;
  }

  const size_t actual_size = header_size + sizeof(h) + rrm.size;

  if (payload) {
    if (actual_size > size) {
      err = E::NOBUFS;
      return -1;
    }

    uint8_t* ptr = reinterpret_cast<uint8_t*>(payload);
    ptr += header_size;
    memcpy(ptr, &h, sizeof(h));
    if (rrm.size) {
      memcpy(ptr + sizeof(h), rrm.data, rrm.size);
    }
  }

  return actual_size;
}

bool SHARD_NEEDS_REBUILD_Event::
operator==(const SHARD_NEEDS_REBUILD_Event& other) const {
  static_assert(std::is_trivially_copyable<SHARD_NEEDS_REBUILD_Header>::value,
                "Header must be trivially copyable");
  return (memcmp(&header, &other.header, sizeof(header)) == 0) &&
      time_ranges == other.time_ranges;
}

std::string SHARD_IS_REBUILT_Header::describe() const {
  std::vector<std::string> flags_str;
  if (flags & SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE) {
    flags_str.push_back("NON_AUTHORITATIVE");
  }

  std::string res = "SHARD_IS_REBUILT(";
  res += "donorNodeIdx=" + folly::to<std::string>(donorNodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ", version=" + lsn_to_string(version);
  res += ", flags=" + folly::join("|", flags_str);
  res += ")";
  return res;
}

template <>
int SHARD_IS_REBUILT_Event::fromPayload(event_log_record_version_t version,
                                        Payload payload,
                                        std::unique_ptr<EventLogRecord>& out) {
  const size_t header_size =
      sizeof(event_log_record_version_t) + sizeof(EventType);

  size_t min_expected_size = sizeof(SHARD_IS_REBUILT_Header);
  if (version < SHARD_IS_REBUILT_FLAGS_VERSION) {
    // support older version that did not have the flags attribute.
    min_expected_size = offsetof(SHARD_IS_REBUILT_Header, flags);
  }

  if (payload.size() < header_size + min_expected_size) {
    ld_error("Invalid record in event log: expecting at least %zu bytes "
             "after the header",
             min_expected_size);
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  ptr += header_size;
  SHARD_IS_REBUILT_Header header = {};
  memcpy(&header, ptr, min_expected_size);

  out = std::make_unique<SHARD_IS_REBUILT_Event>(header, version);

  return 0;
}

std::string SHARD_ACK_REBUILT_Header::describe() const {
  std::string res = "SHARD_ACK_REBUILT(";
  res += "nodeIdx=" + folly::to<std::string>(nodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ", version=" + lsn_to_string(version);
  res += ")";
  return res;
}

std::string SHARD_DONOR_PROGRESS_Header::describe() const {
  std::string res = "SHARD_DONOR_PROGRESS(";
  res += "donorNodeIdx=" + folly::to<std::string>(donorNodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ", timestamp=" + folly::to<std::string>(nextTimestamp);
  res += ", version=" + lsn_to_string(version);
  res += ")";
  return res;
}

std::string SHARD_ABORT_REBUILD_Header::describe() const {
  std::string res = "SHARD_ABORT_REBUILD(";
  res += "nodeIdx=" + folly::to<std::string>(nodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ", version=" + lsn_to_string(version);
  res += ")";
  return res;
}

std::string SHARD_UNDRAIN_Header::describe() const {
  std::string res = "SHARD_UNDRAIN(";
  res += "nodeIdx=" + folly::to<std::string>(nodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ")";
  return res;
}

std::string SHARD_UNRECOVERABLE_Header::describe() const {
  std::string res = "SHARD_UNRECOVERABLE(";
  res += "nodeIdx=" + folly::to<std::string>(nodeIdx);
  res += ", shardIdx=" + folly::to<std::string>(shardIdx);
  res += ")";
  return res;
}

}} // namespace facebook::logdevice
