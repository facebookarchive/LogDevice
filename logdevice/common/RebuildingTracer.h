/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/small_vector.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class TraceLogger;
struct RebuildingSet;

constexpr auto REBUILDING_TRACER = "rebuilding";

class RebuildingTracer : SampledTracer {
 public:
  explicit RebuildingTracer(std::shared_ptr<TraceLogger> logger);

  void traceRecordRebuild(const logid_t logid,
                          const lsn_t lsn,
                          const lsn_t rebuilding_version,
                          const int64_t latency_us,
                          const RebuildingSet& rebuilding_set,
                          const copyset_t& existing_copyset,
                          const copyset_t& new_copyset,
                          const uint32_t waves,
                          const size_t payload_size,
                          const int cur_stage,
                          const char* event_type,
                          const char* status);

  void traceRecordRebuildingAmend(const logid_t logid,
                                  const lsn_t lsn,
                                  const lsn_t rebuilding_version,
                                  const int64_t latency_us,
                                  const RebuildingSet& rebuilding_set,
                                  const uint32_t waves,
                                  const char* event_type,
                                  const char* status);
};

}} // namespace facebook::logdevice
