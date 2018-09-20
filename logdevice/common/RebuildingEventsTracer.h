/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class TraceLogger;
struct RebuildingSet;
struct RebuildingSettings;

constexpr auto REBUILDING_EVENTS_TRACER = "rebuilding_events";

class RebuildingEventsTracer {
 public:
  explicit RebuildingEventsTracer(const std::shared_ptr<TraceLogger> logger);

  void
  traceLogRebuilding(const std::chrono::system_clock::duration& start_timestamp,
                     const int64_t latency_ms,
                     const logid_t logid,
                     const RebuildingSet& replication_set,
                     const lsn_t version,
                     const lsn_t until_lsn,
                     const size_t n_bytes_replicated,
                     const size_t n_records_replicated,
                     const size_t n_records_rebuilding_in_flight,
                     const std::string& event_type);

  static const std::string STARTED;
  static const std::string COMPLETED;
  static const std::string ABORTED;
  static const std::string FAILED_ON_LOGSTORE;
  static const std::string UNEXPECTED_FAILURE;
  static const std::string WINDOW_END_REACHED;

 private:
  const std::shared_ptr<TraceLogger> logger_;
};

}} // namespace facebook::logdevice
