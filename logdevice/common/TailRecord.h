/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/SerializableData.h"
#include "logdevice/common/ZeroCopiedRecord.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file  Definition of a LogDevice record that is usually used to represent
 *        the tail of a log or an epoch. This is similar to the Record
 *        definition in include/Record.h, but it is used internally throughout
 *        LogDevice, e.g., in
 *           - object members (Sequencer, LogRecovery, ...)
 *           - messages (SEALED, GET_SEQ_STATE, ...)
 *           - persistent metadata (EpochStore file, record cache snapshot, ...)
 *
 *       Serialization and deserializaton methods are provided for both socket
 *       evbuffer and linear buffer. Backward and forward compatibility are
 *       achieved by having a fix-sized header and a variable-sized blob
 *       controlled by the flags in the header.
 */

// if the record payload size is less or equal than the limit, and the log is
// a tail optimized log, then logdevice will try to include the payload in
// various messages (e.g., SEALED reply) as well as in epoch store entries
// (i.e., last clean epoch) as latency and efficiency optimization
static constexpr size_t TAIL_RECORD_INLINE_LIMIT = 2048;

struct TailRecordHeader {
  using flags_t = uint32_t;
  using blob_size_t = uint32_t;
  using payload_size_t = uint32_t;

  logid_t log_id;
  lsn_t lsn;
  uint64_t timestamp;

  // How many bytes were written into the log (to which this record belongs)
  // up to this record since the beginning of the log (byte_offset) or the
  // beginning of the epoch (epoch_size). BYTE_OFFSET_INVALID if the information
  // is not available. The OFFSET_WITHIN_EPOCH flags decide the meaning of the
  // value.
  // TODO(T35659884) : remove union from header, this variable is not used as it
  // is replaced with OffsetMap. Can simply delete all places except in
  // serialize and deserialize of TailRecord, more work is needed there.
  union {
    uint64_t byte_offset_DEPRECATED;
    uint64_t offset_within_epoch_DEPRECATED;
  } u_DEPRECATED;

  flags_t flags;
  uint8_t pad[4];

  /////////// Flags //////////

  // if set, the serialization of the record contains an additional blob
  // prefixed by its length of blob_size_t, only used in serialization and
  // deserialization.
  static const flags_t INCLUDE_BLOB = 1u << 0; //=1

  // if set, the record has payload associated with it, in serialization format,
  // the header is followed by payload size of payload_size_t and then the
  // actual payload.
  static const flags_t HAS_PAYLOAD = 1u << 1; //=2

  // payload checksum flags, same as these in RECORD_flags_t, only
  // useful when HAS_PAYLOAD is set
  static const flags_t CHECKSUM = 1u << 2;        //=4
  static const flags_t CHECKSUM_64BIT = 1u << 3;  //=8
  static const flags_t CHECKSUM_PARITY = 1u << 4; //=16

  // the tail record is no longer available, possible reason include dataloss
  // or getting trimmed.
  static const flags_t GAP = 1u << 5; //=32

  // if set, the offset with in epoch (current epoch size) is stored in the
  // header instead of accumulateive byte offset
  static const flags_t OFFSET_WITHIN_EPOCH = 1u << 9; //=512

  // if set, an OffsetMap is present after the payload. `u` will
  // however be populated to preserve backward compatibility.
  // Old servers that do not understand this flag will read `u`
  // and discard the trailing bytes at the end.
  static const flags_t OFFSET_MAP = 1u << 10; //=1024

  static const flags_t ALL_KNOWN_FLAGS = INCLUDE_BLOB | HAS_PAYLOAD | CHECKSUM |
      CHECKSUM_64BIT | CHECKSUM_PARITY | GAP | OFFSET_WITHIN_EPOCH | OFFSET_MAP;
};

static_assert(sizeof(TailRecordHeader) == 40,
              "TailRecordHeader is not packed.");

// these passthrough flags should match end-to-end
static_assert(TailRecordHeader::CHECKSUM == RECORD_Header::CHECKSUM &&
                  TailRecordHeader::CHECKSUM_64BIT ==
                      RECORD_Header::CHECKSUM_64BIT &&
                  TailRecordHeader::CHECKSUM_PARITY ==
                      RECORD_Header::CHECKSUM_PARITY,
              "Flag constants don't match");

class TailRecord : public SerializableData {
 public:
  using SerializableData::deserialize;
  using SerializableData::serialize;

  explicit TailRecord()
      : header{LOGID_INVALID,
               LSN_INVALID,
               0,
               {BYTE_OFFSET_INVALID /* deprecated, offsets_map used instead */},
               0,
               {}} {}

  // use compiler generated copy constructor and assignment operator
  // since members can be safely copied with low cost
  TailRecord(const TailRecord& rhs) noexcept = default;
  TailRecord& operator=(const TailRecord& rhs) noexcept = default;

  TailRecord(TailRecord&& rhs) noexcept;
  TailRecord& operator=(TailRecord&& rhs) noexcept;

  /**
   * Construct a TailRecord with an OffsetMap and an optional linear payload.
   * Note: payload must be backed by linear buffer. For evbuffer-based
   * payload, use the constructor below.
   */
  TailRecord(const TailRecordHeader& header,
             OffsetMap offset_map,
             std::shared_ptr<PayloadHolder> payload);

  /**
   * Construct a TailRecord with and OffsetMap and an optional evbuffer-based
   * payload encapsulated in a ZeroCopiedRecord
   */
  TailRecord(const TailRecordHeader& header,
             OffsetMap offset_map,
             std::shared_ptr<ZeroCopiedRecord> record);

  bool hasPayload() const {
    return header.flags & TailRecordHeader::HAS_PAYLOAD;
  }

  bool isValid() const {
    return header.log_id != LOGID_INVALID &&
        (!hasPayload() || (!payload_ != !zero_copied_record_));
  }

  bool containOffsetWithinEpoch() const {
    return header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH;
  }

  bool containsOffsetMap() const {
    return header.flags & TailRecordHeader::OFFSET_MAP;
  }

  void reset(const TailRecordHeader& h,
             std::shared_ptr<PayloadHolder> payload,
             OffsetMap offset_map) {
    header = h;
    payload_ = std::move(payload);
    zero_copied_record_.reset();
    offsets_map_ = std::move(offset_map);
  }

  void reset(const TailRecordHeader& h,
             std::shared_ptr<ZeroCopiedRecord> record,
             OffsetMap offset_map) {
    header = h;
    payload_.reset();
    zero_copied_record_ = std::move(record);
    offsets_map_ = std::move(offset_map);
  }

  void reset() {
    header = {LOGID_INVALID,
              LSN_INVALID,
              0,
              {BYTE_OFFSET_INVALID /* deprecated, offsets_map_ used instead */},
              0,
              {}};
    payload_.reset();
    zero_copied_record_.reset();
    OffsetMap om;
    offsets_map_ = std::move(om);
  }

  void removePayload() {
    header.flags &= ~TailRecordHeader::HAS_PAYLOAD;
    // Clear checksum flags if we don't ship payload
    header.flags &=
        ~(TailRecordHeader::CHECKSUM | TailRecordHeader::CHECKSUM_64BIT);
    header.flags |= TailRecordHeader::CHECKSUM_PARITY;

    payload_.reset();
    zero_copied_record_.reset();
  }

  /**
   * @return    Slice for the record payload, (nullptr, 0) if object does
   *            not contain payload. must be called with a valid object
   */
  Slice getPayloadSlice() const;

  /**
   * See SerializableData::serialize().
   */
  void serialize(ProtocolWriter& writer) const override;

  /**
   * See SerializableData::deserialize().
   */
  void
  deserialize(ProtocolReader& reader,
              bool evbuffer_zero_copy,
              folly::Optional<size_t> expected_size = folly::none) override;

  /**
   * See SerializableData::name().
   */
  const char* name() const override {
    return "TailRecord";
  }

  /**
   * @return  a human readable string that describes the tail record
   */
  std::string toString() const;

  /**
   * Check if the record has the same content from another TailRecord given.
   * Invalid records are considered same.
   */
  bool sameContent(const TailRecord& rhs) const;
  static bool sameContent(const TailRecord& lhs, const TailRecord& rhs);

  TailRecordHeader header;

  // Keep track of offsets. If flags_t OFFSET_WITHIN_EPOCH is set, the value in
  // offsets_map_ would be the offset_within_epoch else it would represent the
  // byte_offset
  OffsetMap offsets_map_;

 private:
  // container of the actual payload, can be one of
  // 1) a flat, self-owned payload
  // 2) ZeroCopiedRecord containing zero-copied evbuffer
  std::shared_ptr<PayloadHolder> payload_;
  std::shared_ptr<ZeroCopiedRecord> zero_copied_record_;

  // calculate the size of the variable length blob for serialization
  TailRecordHeader::blob_size_t calculateBlobSize() const;

  static size_t
  expectedRecordSizeInBuffer(const TailRecordHeader& header,
                             TailRecordHeader::blob_size_t blob_size,
                             const OffsetMap& offsets_map) {
    return sizeof(TailRecordHeader) +
        (blob_size == 0 ? 0 : (sizeof(blob_size) + blob_size)) +
        (header.flags & TailRecordHeader::OFFSET_MAP
             ? offsets_map.sizeInLinearBuffer()
             : 0);
  }
};

}} // namespace facebook::logdevice
