/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <stdint.h>

#include "logdevice/common/checks.h"
#include "logdevice/common/toString.h"

/**
 * @file  Types of LogsConfigApiRequest message origins
 */

namespace facebook { namespace logdevice {

enum class LogsConfigRequestOrigin : uint8_t {
  LOGS_CONFIG_API_REQUEST = 0,
  REMOTE_LOGS_CONFIG_REQUEST = 1,
};

inline std::string toString(LogsConfigRequestOrigin val) {
  switch (val) {
    case LogsConfigRequestOrigin::LOGS_CONFIG_API_REQUEST:
      return "LOGS_CONFIG_API_REQUEST";
    case LogsConfigRequestOrigin::REMOTE_LOGS_CONFIG_REQUEST:
      return "REMOTE_LOGS_CONFIG_REQUEST";
  }
  ld_check(false);
  return "(internal error!)";
}

}} // namespace facebook::logdevice
