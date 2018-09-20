/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/EnumMap.h"

/**
 * @file types of requests that can be executed on EventLoop threads.
 *       We maintain an enum of types especially for maintaining per request
 *       type stats.
 */

namespace facebook { namespace logdevice {

enum class RequestType : unsigned char {
  INVALID,
#define REQUEST_TYPE(name) name,
#include "logdevice/common/request_types.inc"
#define REQUEST_TYPE(name) name,
#include "logdevice/common/test_request_types.inc"
  MAX
};

static_assert(sizeof(RequestType) == 1, "RequestType must be 1 byte");

extern EnumMap<RequestType, std::string> requestTypeNames;

}} // namespace facebook::logdevice
