/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ThrottledTracer.h"

namespace facebook { namespace logdevice {

constexpr auto PURGING_TRACER = "purging";

class PurgingTracer {
 public:
  // Uses per-thread throttling to limit the rate of publishing samples.
  static void traceRecordPurge(TraceLogger* logger,
                               logid_t logid,
                               epoch_t epoch,
                               esn_t esn,
                               esn_t start_esn,
                               esn_t end_esn,
                               bool v2);
};

}} // namespace facebook::logdevice
