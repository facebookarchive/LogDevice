/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeHealthStatus.h"

namespace facebook { namespace logdevice {

std::string toString(NodeHealthStatus status) {
  switch (status) {
    case NodeHealthStatus::HEALTHY:
      return "HEALTHY";
    case NodeHealthStatus::OVERLOADED:
      return "OVERLOADED";
    case NodeHealthStatus::UNHEALTHY:
      return "UNHEALTHY";
    case NodeHealthStatus::UNDEFINED:
      return "UNDEFINED";
  }
  return "UNKNOWN";
}

}} // namespace facebook::logdevice
