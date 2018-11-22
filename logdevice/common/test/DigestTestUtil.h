/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Memory.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Record.h"

/**
 * @file     a collection of test util functions for digest and epoch recovery
 *           related test
 */

namespace facebook { namespace logdevice { namespace DigestTestUtil {

constexpr lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

constexpr lsn_t lsn(epoch_t epoch, int esn) {
  return compose_lsn(epoch, esn_t(esn));
}

constexpr lsn_t lsn(epoch_t epoch, esn_t esn) {
  return compose_lsn(epoch, esn);
}

enum RecordType {
  // normal record stored by Appender
  NORMAL = 0,
  // mutated record stored by epoch recovery
  MUTATED,
  // hole plug stored by epoch recovery
  HOLE,
  // bridge record, a special hole plug stored by epoch recovery
  BRIDGE
};

// TODO 11866467: support for specifying copyset
std::unique_ptr<DataRecordOwnsPayload> create_record(
    logid_t logid,
    lsn_t lsn,
    RecordType type,
    uint32_t wave_or_seal_epoch,
    std::chrono::milliseconds timestamp = std::chrono::milliseconds(0),
    size_t payload_size = 128,
    OffsetMap offsets_within_epoch = OffsetMap(),
    OffsetMap offsets = OffsetMap());

}}} // namespace facebook::logdevice::DigestTestUtil
