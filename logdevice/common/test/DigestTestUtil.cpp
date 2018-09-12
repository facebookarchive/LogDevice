/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "DigestTestUtil.h"

namespace facebook { namespace logdevice { namespace DigestTestUtil {

std::unique_ptr<DataRecordOwnsPayload>
create_record(logid_t logid,
              lsn_t lsn,
              RecordType type,
              uint32_t wave_or_seal_epoch,
              std::chrono::milliseconds timestamp,
              size_t payload_size,
              uint64_t offset_within_epoch,
              uint64_t byte_offset) {
  Payload payload;
  if (type != RecordType::HOLE && type != RecordType::BRIDGE) {
    char* buf = (char*)malloc(payload_size);
    snprintf(
        buf, payload_size, "Record with lsn %s", lsn_to_string(lsn).c_str());
    payload = Payload{buf, payload_size};
  }

  RECORD_flags_t flags =
      type != RecordType::NORMAL ? RECORD_Header::WRITTEN_BY_RECOVERY : 0;
  if (type == RecordType::HOLE || type == RecordType::BRIDGE) {
    flags |= RECORD_Header::HOLE;
  }

  if (type == RecordType::BRIDGE) {
    flags |= RECORD_Header::BRIDGE;
  }

  if (offset_within_epoch != BYTE_OFFSET_INVALID) {
    flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
  }

  if (byte_offset != BYTE_OFFSET_INVALID) {
    flags |= RECORD_Header::INCLUDE_BYTE_OFFSET;
  }

  auto extra_metadata = std::make_unique<ExtraMetadata>();
  extra_metadata->header.wave = wave_or_seal_epoch;
  extra_metadata->offset_within_epoch = offset_within_epoch;

  auto record = std::make_unique<DataRecordOwnsPayload>(
      logid,
      std::move(payload),
      lsn,
      timestamp,
      flags,
      std::move(extra_metadata),
      nullptr /* BufferedWriteDecoder */,
      0 /* batch_offset */,
      byte_offset);

  return record;
}

}}} // namespace facebook::logdevice::DigestTestUtil
