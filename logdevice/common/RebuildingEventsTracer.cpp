/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RebuildingEventsTracer.h"

#include <chrono>

#include <folly/Memory.h>

#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/TraceSample.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice {

// const std::string REBUILDING_EVENTS_TABLE = "rebuilding_events";

const std::string RebuildingEventsTracer::STARTED = "started";
const std::string RebuildingEventsTracer::COMPLETED = "completed";
const std::string RebuildingEventsTracer::ABORTED = "aborted";
const std::string RebuildingEventsTracer::FAILED_ON_LOGSTORE =
    "failed_to_read_from_logstore";
const std::string RebuildingEventsTracer::UNEXPECTED_FAILURE =
    "unexpected_failure";
const std::string RebuildingEventsTracer::WINDOW_END_REACHED =
    "window_end_reached";

RebuildingEventsTracer::RebuildingEventsTracer(
    const std::shared_ptr<TraceLogger> logger)
    : logger_(logger) {}

void RebuildingEventsTracer::traceLogRebuilding(
    const std::chrono::system_clock::duration& start_timestamp,
    const int64_t latency_ms,
    const logid_t logid,
    const RebuildingSet& rebuilding_set,
    const lsn_t version,
    const lsn_t until_lsn,
    const size_t n_bytes_replicated,
    const size_t n_records_replicated,
    const size_t n_records_rebuilding_in_flight,
    const std::string& event_type) {
  if (logger_) {
    auto sample = std::make_unique<TraceSample>();

    std::vector<std::string> rebuilding_shards;
    for (auto& rec : rebuilding_set.shards) {
      rebuilding_shards.push_back(rec.first.toString());
    }

    std::shared_ptr<Configuration> cfg = logger_->getConfiguration();
    std::string log_range = cfg->getLogGroupPath(logid).value_or("<UNKNOWN>");
    sample->addIntValue(
        "start_timestamp",
        std::chrono::duration_cast<std::chrono::seconds>(start_timestamp)
            .count());
    sample->addIntValue("latency_ms", latency_ms);
    sample->addIntValue("log_id", logid.val());
    sample->addNormalValue("log_range", log_range);
    sample->addNormVectorValue("rebuilding_set", rebuilding_shards);
    // RebuildingSettings
    sample->addNormalValue("rebuilding_version", std::to_string(version));
    sample->addNormalValue("until_lsn", std::to_string(until_lsn));
    sample->addIntValue("n_bytes_replicated", n_bytes_replicated);
    sample->addIntValue("n_records_replicated", n_records_replicated);
    sample->addIntValue(
        "n_records_rebuilding_in_flight", n_records_rebuilding_in_flight);
    sample->addNormalValue("event_type", event_type);
    logger_->pushSample(REBUILDING_EVENTS_TRACER, 1, std::move(sample));
  }
}
}} // namespace facebook::logdevice
