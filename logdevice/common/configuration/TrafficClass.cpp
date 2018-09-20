/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/TrafficClass.h"

namespace facebook { namespace logdevice {

EnumMap<TrafficClass, std::string>& trafficClasses() {
  static EnumMap<TrafficClass, std::string> _trafficClasses;
  return _trafficClasses;
}

template <>
/* static */
const std::string& EnumMap<TrafficClass, std::string>::invalidValue() {
  static const std::string invalidTrafficClassName("");
  return invalidTrafficClassName;
}

template <>
void EnumMap<TrafficClass, std::string>::setValues() {
#define TRAFFIC_CLASS(c) set(TrafficClass::c, #c);
#include "logdevice/common/traffic_classes.inc"
}

}} // namespace facebook::logdevice
