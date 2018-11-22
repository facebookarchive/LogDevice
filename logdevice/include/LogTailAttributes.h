/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstdint>

#include "folly/Format.h"
#include "logdevice/include/RecordOffset.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {
/**
 * Structure that contains attributes of the tail of the log.
 * It includes:
 *  last_released_real_lsn    Sequence number of last written and released for
 *                            delivery record of the log.
 *  last_timestamp  Estimated timestamp of record with last_released_real_lsn
 *                  sequence number. It may be slightly larger than real
 *                  timestamp of a record with last_released_real_lsn lsn.
 *  offsets         RecordOffset object containing a map of <counter_type_t,
 *                  value> Currently supports BYTE_OFFSET. BYTE_OFFSET
 *                  represents the amount of data in bytes written from the
 *                  beginning of the log up to the end.
 */

struct LogTailAttributes {
  lsn_t last_released_real_lsn;

  std::chrono::milliseconds last_timestamp;

  RecordOffset offsets;

  LogTailAttributes() noexcept
      : last_released_real_lsn(LSN_INVALID),
        last_timestamp(std::chrono::milliseconds{0}) {}

  LogTailAttributes(lsn_t last_released_lsn,
                    std::chrono::milliseconds lts,
                    RecordOffset ro) noexcept
      : last_released_real_lsn(last_released_lsn),
        last_timestamp(lts),
        offsets(std::move(ro)) {}

  LogTailAttributes& operator=(const LogTailAttributes& tail) = default;

  // This can return false if the log is empty
  bool valid() const noexcept {
    // offsets can be invalid if byte_offsets feature is turned
    // off
    return last_released_real_lsn != LSN_INVALID &&
        last_timestamp != std::chrono::milliseconds{0};
  }

  std::string toString() const {
    return folly::sformat(
        "last_released_real_lsn={:s},last_timestamp={:d},offsets={:s}",
        lsn_to_string(last_released_real_lsn),
        last_timestamp.count(),
        offsets.toString().c_str());
  }
};

}} // namespace facebook::logdevice
