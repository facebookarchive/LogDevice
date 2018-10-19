/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class TraceLogger;
class RecipientSet;
struct ClientID;

constexpr auto APPENDER_TRACER = "appender";

class AppenderTracer : SampledTracer {
 public:
  explicit AppenderTracer(std::shared_ptr<TraceLogger> logger);

  // If you're adding columns here, don't forget to update FBTraceLogger.cpp
  void traceAppend(size_t full_appender_size,
                   epoch_t seen_epoch,
                   bool chaining,
                   const ClientID& client_id,
                   const Sockaddr& client_sock_addr,
                   int64_t latency_us,
                   RecipientSet& recipient_set,
                   logid_t log_id,
                   lsn_t lsn,
                   folly::Optional<std::chrono::seconds> backlog_duration,
                   uint32_t waves,
                   std::string client_status,
                   std::string internal_status);
};

}} // namespace facebook::logdevice
