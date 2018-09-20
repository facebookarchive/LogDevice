/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Priority.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/include/EnumMap.h"

namespace facebook { namespace logdevice {

/**
 * @file  Mapping functions converting to and from Priority values.
 */

class PriorityMap {
 public:
  // Map from Priority to std::string.
  using NameMap = EnumMap<Priority,
                          std::string,
                          Priority::INVALID,
                          asInt(Priority::NUM_PRIORITIES)>;

  static NameMap& toName();

  // Map from TrafficClass to Priority.
  static EnumMap<TrafficClass, Priority>& fromTrafficClass();
};

}} // namespace facebook::logdevice
