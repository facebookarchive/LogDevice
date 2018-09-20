/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <cstdlib>
#include <string>

#include "logdevice/include/EnumMap.h"

/**
 * @file    Traffic classes used for resource prioritization.
 */

namespace facebook { namespace logdevice {

enum class TrafficClass : uint8_t {
#define TRAFFIC_CLASS(name) name,
#include "logdevice/common/traffic_classes.inc"
  MAX,
  INVALID
};

extern EnumMap<TrafficClass, std::string>& trafficClasses();

constexpr std::array<TrafficClass, 2> read_traffic_classes{
    {TrafficClass::READ_TAIL, TrafficClass::READ_BACKLOG}};

}} // namespace facebook::logdevice
