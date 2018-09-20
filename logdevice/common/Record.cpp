/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

std::string gapTypeToString(GapType type) {
#define LD_GAP_TYPE_TO_STRING(t) \
  case GapType::t:               \
    return #t;
  switch (type) {
    LD_GAP_TYPE_TO_STRING(UNKNOWN)
    LD_GAP_TYPE_TO_STRING(BRIDGE)
    LD_GAP_TYPE_TO_STRING(HOLE)
    LD_GAP_TYPE_TO_STRING(DATALOSS)
    LD_GAP_TYPE_TO_STRING(TRIM)
    LD_GAP_TYPE_TO_STRING(ACCESS)
    LD_GAP_TYPE_TO_STRING(NOTINCONFIG)
    LD_GAP_TYPE_TO_STRING(FILTERED_OUT)
    LD_GAP_TYPE_TO_STRING(MAX)
    default:
      return "UNDEFINED";
  }
#undef LD_GAP_TYPE_TO_STRING
}

}} // namespace facebook::logdevice
