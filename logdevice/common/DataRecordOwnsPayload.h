/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <variant>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

class BufferedWriteDecoder;
struct ExtraMetadata;

/**
 * Like DataRecord, but owns payload and has some additional fields not exposed
 * through public API. Payload is pwned in one of two ways: PayloadHolder
 * or shared_ptr<BufferedWriterDecoder>. The latter is used for records that are
 * part of a batch; such records share ownership of the bigger record containing
 * the whole batch.
 */
struct DataRecordOwnsPayload : public DataRecord {
  DataRecordOwnsPayload(logid_t log_id,
                        PayloadHolder&& payload_holder,
                        lsn_t lsn,
                        std::chrono::milliseconds timestamp,
                        RECORD_flags_t flags,
                        std::unique_ptr<ExtraMetadata> extra_metadata = nullptr,
                        int batch_offset = 0,
                        RecordOffset offsets = RecordOffset(),
                        bool invalid_checksum = false);

  DataRecordOwnsPayload(logid_t log_id,
                        Payload payload,
                        std::shared_ptr<BufferedWriteDecoder> decoder,
                        lsn_t lsn,
                        std::chrono::milliseconds timestamp,
                        RECORD_flags_t flags,
                        std::unique_ptr<ExtraMetadata> extra_metadata = nullptr,
                        int batch_offset = 0,
                        RecordOffset offsets = RecordOffset(),
                        bool invalid_checksum = false);

  ~DataRecordOwnsPayload();

  // flags extracted from the RECORD message
  RECORD_flags_t flags_;

  // true if this record's supplied checksum didn't match the calculated
  // checksum
  bool invalid_checksum_ = false;

  // Additional metadata if part of recovery or rebuilding
  const std::unique_ptr<ExtraMetadata> extra_metadata_;

  // Information on how to delete the payload.
  std::variant<PayloadHolder, std::shared_ptr<BufferedWriteDecoder>> owner_;
};

}} // namespace facebook::logdevice
