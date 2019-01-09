/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DataRecordOwnsPayload.h"

#include <cstdlib>

#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/BufferedWriteDecoder.h"

namespace facebook { namespace logdevice {

DataRecordOwnsPayload::DataRecordOwnsPayload(
    logid_t log_id,
    Payload&& payload,
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    RECORD_flags_t flags,
    std::unique_ptr<ExtraMetadata> extra_metadata,
    std::shared_ptr<BufferedWriteDecoder> decoder,
    int batch_offset,
    RecordOffset offsets,
    bool invalid_checksum)
    : DataRecord(log_id,
                 std::move(payload),
                 lsn,
                 timestamp,
                 batch_offset,
                 std::move(offsets)),
      flags_(flags),
      invalid_checksum_(invalid_checksum),
      extra_metadata_(std::move(extra_metadata)),
      decoder_(std::move(decoder)) {}

DataRecordOwnsPayload::~DataRecordOwnsPayload() {
  if (!decoder_) {
    if (payload.data()) {
      free(const_cast<void*>(payload.data()));
    } else {
      ld_check(payload.size() == 0);
    }
  }
}

}} // namespace facebook::logdevice
