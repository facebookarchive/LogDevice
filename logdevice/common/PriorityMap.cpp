/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PriorityMap.h"

namespace facebook { namespace logdevice {

PriorityMap::NameMap& PriorityMap::toName() {
  static PriorityMap::NameMap _toName;
  return _toName;
}

template <>
/* static */
const std::string& PriorityMap::NameMap::invalidValue() {
  static const std::string invalidPriorityName("");
  return invalidPriorityName;
}

template <>
void PriorityMap::NameMap::setValues() {
#define PRIORITY(c) set(Priority::c, #c);
#include "logdevice/common/priorities.inc"
}

EnumMap<TrafficClass, Priority>& PriorityMap::fromTrafficClass() {
  static EnumMap<TrafficClass, Priority> _fromTrafficClass;
  return _fromTrafficClass;
}

template <>
const Priority& EnumMap<TrafficClass, Priority>::invalidValue() {
  static const Priority invalidPriority(Priority::INVALID);
  return (invalidPriority);
};

// Define priorities for all traffic classes.
//
// The function template scheme used here ensures there will be a
// compile time failure if a new traffic class is added without being
// assigned a default priority.
template <TrafficClass Tc>
constexpr Priority trafficClassToPriority();

#define TRAFFIC_CLASS_TO_PRIORITY(TC, P)                          \
  template <>                                                     \
  constexpr Priority trafficClassToPriority<TrafficClass::TC>() { \
    return Priority::P;                                           \
  }

TRAFFIC_CLASS_TO_PRIORITY(HANDSHAKE, MAX)
TRAFFIC_CLASS_TO_PRIORITY(FAILURE_DETECTOR, MAX)
TRAFFIC_CLASS_TO_PRIORITY(RECOVERY, MAX)
TRAFFIC_CLASS_TO_PRIORITY(RSM, MAX)
TRAFFIC_CLASS_TO_PRIORITY(APPEND, CLIENT_HIGH)
TRAFFIC_CLASS_TO_PRIORITY(TRIM, CLIENT_HIGH)
TRAFFIC_CLASS_TO_PRIORITY(READ_TAIL, CLIENT_NORMAL)
TRAFFIC_CLASS_TO_PRIORITY(READ_BACKLOG, CLIENT_LOW)
TRAFFIC_CLASS_TO_PRIORITY(REBUILD, BACKGROUND)
#undef TRAFFIC_CLASS_TO_PRIORITY

template <>
void EnumMap<TrafficClass, Priority>::setValues() {
#define TRAFFIC_CLASS(name) \
  set(TrafficClass::name, trafficClassToPriority<TrafficClass::name>());
#include "logdevice/common/traffic_classes.inc"
}

}} // namespace facebook::logdevice
