/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientGapTracer.h"

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/*
 * By default limits the number of trace messages to 1 per second and
 * automatically adjusts the reported sample rate
 */
ClientGapTracer::ClientGapTracer(const std::shared_ptr<TraceLogger> logger)
    : ThrottledTracer(logger, GAP_TRACER, std::chrono::seconds(10), 10) {}

void ClientGapTracer::traceGapDelivery(const logid_t logid,
                                       const GapType type,
                                       const lsn_t lo,
                                       const lsn_t hi,
                                       const lsn_t start_lsn,
                                       const std::string& epoch_metadata,
                                       const std::string& gap_state,
                                       const std::string& unavailable_nodes,
                                       const epoch_t epoch,
                                       const lsn_t trim_point,
                                       const size_t readset_size,
                                       const std::string& replication_factor) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addIntValue("log_id", logid.val());
    sample->addNormalValue("type", gapTypeToString(type));
    sample->addNormalValue("from_lsn", std::to_string(lo));
    sample->addNormalValue("to_lsn", std::to_string(hi));
    sample->addNormalValue("start_lsn", std::to_string(start_lsn));
    sample->addNormalValue("epoch_metadata", epoch_metadata);
    sample->addNormalValue("gap_state", gap_state);
    sample->addNormalValue("unavailable_nodes", unavailable_nodes);
    sample->addNormalValue("epoch", std::to_string(epoch.val()));
    sample->addNormalValue("trim_point", std::to_string(trim_point));
    sample->addIntValue("readset_size", readset_size);
    sample->addNormalValue("replication_property", replication_factor);
    return sample;
  };
  publish(sample_builder);
}
}} // namespace facebook::logdevice
