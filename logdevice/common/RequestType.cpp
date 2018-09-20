/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RequestType.h"

namespace facebook { namespace logdevice {

EnumMap<RequestType, std::string> requestTypeNames;

template <>
const std::string& EnumMap<RequestType, std::string>::invalidValue() {
  static const std::string invalidName("");
  return invalidName;
}

template <>
void EnumMap<RequestType, std::string>::setValues() {
#define REQUEST_TYPE(name) set(RequestType::name, #name);
#include "logdevice/common/request_types.inc"
#define REQUEST_TYPE(name) set(RequestType::name, #name);
#include "logdevice/common/test_request_types.inc"
}

}} // namespace facebook::logdevice
