/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

class BufferedWriteDecoder;
struct ExtraMetadata;

/**
 * Simple wrapper around DataRecord that owns the payload in one of two ways:
 * - Unique ownership, when decoder_ is null.  This is the most common;
 *   DataRecordOwnsPayload will free() the payload.
 * - Shared ownership, when decoder_ is non-null.  This record is part of a
 *   group that was decoded together; decoder_ owns the memory for all of
 *   them.
 */
struct DataRecordOwnsPayload : public DataRecord {
  /**
   * If `decoder' is null, takes ownership of the payload contained in the
   * record.  The payload must have been allocated with malloc().
   *
   * If `decoder' is non-null, `payload' is expected to be a soft pointer into
   * memory owned by the decoder.
   */
  explicit DataRecordOwnsPayload(logid_t log_id,
                                 Payload&& payload,
                                 lsn_t lsn,
                                 std::chrono::milliseconds timestamp,
                                 RECORD_flags_t flags,
                                 std::unique_ptr<ExtraMetadata> extra_metadata =
                                     std::unique_ptr<ExtraMetadata>(),
                                 std::shared_ptr<BufferedWriteDecoder> decoder =
                                     std::shared_ptr<BufferedWriteDecoder>(),
                                 int batch_offset = 0,
                                 RecordOffset offsets = RecordOffset(),
                                 bool invalid_checksum = false);

  ~DataRecordOwnsPayload() override;

  // flags extracted from the RECORD message
  RECORD_flags_t flags_;

  // true if this record's supplied checksum didn't match the calculated
  // checksum
  bool invalid_checksum_ = false;

  // Additional metadata if part of recovery or rebuilding
  const std::unique_ptr<ExtraMetadata> extra_metadata_;

  // Decoder that owns memory if sharing ownership with other instances
  const std::shared_ptr<BufferedWriteDecoder> decoder_;
};

}} // namespace facebook::logdevice
