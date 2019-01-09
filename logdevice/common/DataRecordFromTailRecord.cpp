/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DataRecordFromTailRecord.h"

#include "logdevice/common/Checksum.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

DataRecordFromTailRecord::DataRecordFromTailRecord(
    std::shared_ptr<TailRecord> tail)
    : DataRecord(tail->header.log_id,
                 Payload(),
                 tail->header.lsn,
                 std::chrono::milliseconds(tail->header.timestamp),
                 0 /* batch_offset */,
                 OffsetMap::toRecord(tail->offsets_map_)),
      tail_record_(std::move(tail)) {
  ld_check(tail_record_ != nullptr);
  // tail record must be valid and contains payload
  ld_check(tail_record_->isValid());
  ld_check(tail_record_->hasPayload());
  payload = computePayload(*tail_record_, &checksum_failed_);
}

/* static */
std::unique_ptr<DataRecordFromTailRecord>
DataRecordFromTailRecord::create(std::shared_ptr<TailRecord> tail) {
  ld_check(tail != nullptr);
  if (!tail->isValid() || !tail->hasPayload()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Cannot get data record from tail record which is invalid or "
        "does not have record payload. Tail record %s.",
        tail->toString().c_str());
    return nullptr;
  }
  return std::unique_ptr<DataRecordFromTailRecord>(
      new DataRecordFromTailRecord(std::move(tail)));
}

/* static */
Payload DataRecordFromTailRecord::computePayload(const TailRecord& tail,
                                                 bool* checksum_failed) {
  auto flags = tail.header.flags;
  size_t checksum_size =
      ((flags & TailRecordHeader::CHECKSUM)
           ? ((flags & TailRecordHeader::CHECKSUM_64BIT) ? sizeof(uint64_t)
                                                         : sizeof(uint32_t))
           : 0);

  Slice blob_slice = tail.getPayloadSlice();
  if (checksum_size > blob_slice.size) {
    // tail blob size is smaller than the suggested checksum size
    *checksum_failed = true;
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Note enough checksum bytes in tail record %s. "
                    "expected checksum size: %lu, tail blob size: %lu.",
                    tail.toString().c_str(),
                    checksum_size,
                    blob_slice.size);
    return Payload(blob_slice.data, blob_slice.size);
  }

  const Payload payload(
      static_cast<const char*>(blob_slice.data) + checksum_size,
      blob_slice.size - checksum_size);

  // first verify the parity bit
  bool expected_parity = bool(flags & TailRecordHeader::CHECKSUM) ==
      bool(flags & TailRecordHeader::CHECKSUM_64BIT);
  if (expected_parity != bool(flags & TailRecordHeader::CHECKSUM_PARITY)) {
    *checksum_failed = true;
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Checksum parity check failed for tail record %s.",
                    tail.toString().c_str());
    return payload;
  }

  if (!(flags & TailRecordHeader::CHECKSUM)) {
    // no checksum included
    *checksum_failed = false;
    return payload;
  }

  ld_check(checksum_size > 0);
  union {
    uint64_t c64;
    uint32_t c32;
  } u;
  memcpy(&u, blob_slice.data, checksum_size);
  const uint64_t expected_checksum =
      (flags & TailRecordHeader::CHECKSUM_64BIT) ? u.c64 : u.c32;

  const uint64_t payload_checksum = (flags & RECORD_Header::CHECKSUM_64BIT)
      ? checksum_64bit(Slice(payload))
      : checksum_32bit(Slice(payload));

  *checksum_failed = (expected_checksum != payload_checksum);
  if (*checksum_failed) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Checksum failed for tail record %s. Expected checksum: %lu, "
        "Got: %lu. tail blob size: %lu, record payload size %lu.",
        tail.toString().c_str(),
        expected_checksum,
        payload_checksum,
        blob_slice.size,
        payload.size());
  }
  return payload;
}

}} // namespace facebook::logdevice
