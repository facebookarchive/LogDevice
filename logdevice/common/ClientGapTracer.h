/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "logdevice/common/ThrottledTracer.h"

namespace facebook { namespace logdevice {

enum class GapType;

constexpr auto GAP_TRACER = "gap_tracer";
class ClientGapTracer : public ThrottledTracer {
 public:
  explicit ClientGapTracer(const std::shared_ptr<TraceLogger> logger);
  void traceGapDelivery(const logid_t logid,
                        const GapType type,
                        const lsn_t lo,
                        const lsn_t hi,
                        // LSN from which the client started reading
                        const lsn_t start_lsn,
                        // current EpochMetaData
                        const std::string& epoch_metadata,
                        const std::string& gap_state,
                        const std::string& unavailable_nodes,
                        const epoch_t epoch,
                        const lsn_t trim_point,
                        const size_t readset_size,
                        const std::string& replication_factor);
};

}} // namespace facebook::logdevice
