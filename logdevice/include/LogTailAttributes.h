/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstdint>

#include "folly/Format.h"
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
 *  byte_offset     Amount of data in bytes written from the beginning of the
 *                  log up to the end.
 */
struct LogTailAttributes {
  lsn_t last_released_real_lsn;

  std::chrono::milliseconds last_timestamp;

  uint64_t byte_offset;

  LogTailAttributes() noexcept
      : last_released_real_lsn(LSN_INVALID),
        last_timestamp(std::chrono::milliseconds{0}),
        byte_offset(BYTE_OFFSET_INVALID) {}

  LogTailAttributes(lsn_t last_released_lsn,
                    std::chrono::milliseconds lts,
                    uint64_t bo) noexcept
      : last_released_real_lsn(last_released_lsn),
        last_timestamp(lts),
        byte_offset(bo) {}

  LogTailAttributes& operator=(const LogTailAttributes&) = default;

  // This can return false if the log is empty
  bool valid() const noexcept {
    // byte_offset can be BYTE_OFFSET_INVALID if byte_offsets feature is turned
    // off
    return last_released_real_lsn != LSN_INVALID &&
        last_timestamp != std::chrono::milliseconds{0};
  }

  std::string toString() const {
    return folly::sformat(
        "last_released_real_lsn={:s},last_timestamp={:d},byte_offset={:d}",
        lsn_to_string(last_released_real_lsn),
        last_timestamp.count(),
        byte_offset);
  }
};

}} // namespace facebook::logdevice
