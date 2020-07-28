/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * Record retrieved from storage, which has raw (possibly encoded) payload.
 * Payload encoding depends on the flags.
 */
struct RawDataRecord : public LogRecord {
  RawDataRecord(logid_t log_id,
                PayloadHolder&& payload_holder,
                lsn_t lsn,
                std::chrono::milliseconds timestamp,
                RECORD_flags_t flags,
                std::unique_ptr<ExtraMetadata> extra_metadata = nullptr,
                RecordOffset offsets = RecordOffset(),
                bool invalid_checksum = false)
      : LogRecord(log_id),
        payload(std::move(payload_holder)),
        attrs(lsn, timestamp, /* batch_offset */ 0, std::move(offsets)),
        flags(flags),
        invalid_checksum(invalid_checksum),
        extra_metadata(std::move(extra_metadata)) {}

  PayloadHolder payload;      // payload of this record
  DataRecordAttributes attrs; // attributes of this record

  // flags extracted from the RECORD message
  RECORD_flags_t flags;

  // true if this record's supplied checksum didn't match the calculated
  // checksum
  bool invalid_checksum = false;

  // Additional metadata if part of recovery or rebuilding
  std::unique_ptr<ExtraMetadata> extra_metadata;
};

}} // namespace facebook::logdevice
