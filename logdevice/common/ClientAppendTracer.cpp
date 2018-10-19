/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientAppendTracer.h"

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

ClientAppendTracer::ClientAppendTracer(std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void ClientAppendTracer::traceAppend(const logid_t log_id,
                                     size_t payload_size_bytes,
                                     int64_t request_timeout_msec,
                                     Status client_request_status,
                                     Status internal_request_status,
                                     lsn_t lsn,
                                     int64_t request_latency_usec,
                                     lsn_t previous_lsn,
                                     NodeID sequencer) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();

    sample->addNormalValue("log_id", std::to_string(log_id.val()));
    sample->addIntValue("payload_size", payload_size_bytes);
    sample->addIntValue("request_timeout", request_timeout_msec);

    sample->addNormalValue(
        "client_status_code", std::string(error_name(client_request_status)));
    sample->addNormalValue("internal_status_code",
                           std::string(error_name(internal_request_status)));
    sample->addNormalValue("lsn", lsn_to_string(lsn));
    sample->addIntValue("request_latency", request_latency_usec);

    sample->addNormalValue("previous_lsn", lsn_to_string(previous_lsn));
    sample->addNormalValue("sequencer_node", sequencer.toString());

    return sample;
  };
  publish(CLIENT_APPEND_TRACER, sample_builder);
}

}} // namespace facebook::logdevice
