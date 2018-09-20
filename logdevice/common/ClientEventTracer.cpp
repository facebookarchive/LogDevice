/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientEventTracer.h"

#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

ClientEventTracer::ClientEventTracer(std::shared_ptr<TraceLogger> logger,
                                     StatsHolder* stats)
    : ThrottledTracer(std::move(logger),
                      CLIENT_EVENT_TRACER,
                      std::chrono::seconds(10),
                      10),
      stats_(stats) {}

void ClientEventTracer::traceEvent(Severity sev,
                                   std::string name_space,
                                   std::string type,
                                   std::string data,
                                   std::string context) {
  switch (sev) {
    case Severity::CRITICAL:
      STAT_INCR(stats_, client.critical_events);
      break;
    case Severity::ERROR:
      STAT_INCR(stats_, client.error_events);
      break;
    case Severity::WARNING:
      STAT_INCR(stats_, client.notice_events);
      break;
    case Severity::INFO:
      STAT_INCR(stats_, client.info_events);
      break;
    default:
      break;
  }

  auto sample_builder = [&]() {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("event_severity", logdevice::toString(sev));
    sample->addNormalValue("event_namespace", std::move(name_space));
    sample->addNormalValue("event_type", std::move(type));
    sample->addNormalValue("event_data", std::move(data));
    sample->addNormalValue("event_context", std::move(context));
    return sample;
  };
  publish(sample_builder);
}

}} // namespace facebook::logdevice
