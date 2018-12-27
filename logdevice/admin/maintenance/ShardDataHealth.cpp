/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ShardDataHealth.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

std::string toString(const ShardDataHealth& st) {
  switch (st) {
    case ShardDataHealth::HEALTHY:
      return "HEALTHY";
    case ShardDataHealth::UNAVAILABLE:
      return "UNAVAILABLE";
    case ShardDataHealth::LOST_ALL:
      return "LOST_ALL";
    case ShardDataHealth::LOST_REGIONS:
      return "LOST_REGIONS";
    case ShardDataHealth::EMPTY:
      return "EMPTY";
    case ShardDataHealth::Count:
      break;
  }
  ld_check(false);
  return "invalid";
}

}} // namespace facebook::logdevice
