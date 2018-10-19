/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdlib.h>

#include "folly/Format.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * Structure that contains attributes of the head (trim point) of the log.
 */
struct LogHeadAttributes {
  LogHeadAttributes(lsn_t trim_point_,
                    std::chrono::milliseconds trim_point_timestamp_)
      : trim_point(trim_point_), trim_point_timestamp(trim_point_timestamp_) {}

  LogHeadAttributes()
      : trim_point(LSN_INVALID),
        trim_point_timestamp(std::chrono::milliseconds::max()) {}

  // Trim point of the log. Set to LSN_INVALID if log was never trimmed.
  lsn_t trim_point;

  // Approximate timestamp of the next record after trim point. Set to
  // std::chrono::milliseconds::max() if there is no records bigger than
  // trim point.
  std::chrono::milliseconds trim_point_timestamp;

  std::string toString() const {
    return folly::sformat("trim_point={:s},trim_point_timestamp={:d}",
                          lsn_to_string(trim_point),
                          trim_point_timestamp.count());
  }
};

}} // namespace facebook::logdevice
