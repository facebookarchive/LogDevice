/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

class TraceLogger;

constexpr auto CLIENT_APPEND_TRACER = "client_append_tracer";

class ClientAppendTracer : SampledTracer {
 public:
  explicit ClientAppendTracer(std::shared_ptr<TraceLogger> logger);

  void traceAppend(const logid_t log_id,
                   size_t payload_size_bytes,
                   int64_t timeout_msec,
                   Status client_request_status,
                   Status internal_request_status,
                   lsn_t lsn,
                   int64_t latency_usec,
                   lsn_t previous_lsn,
                   NodeID sequencer);
};

}} // namespace facebook::logdevice
