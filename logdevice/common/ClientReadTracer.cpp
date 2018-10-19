/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientReadTracer.h"

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

ClientReadTracer::ClientReadTracer(std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void ClientReadTracer::traceRecordDelivery(
    logid_t logid,
    lsn_t record_lsn,
    read_stream_id_t read_stream_id,
    NodeID from_node_id,
    std::chrono::microseconds epoch_metadata_fetch_latency,
    std::chrono::microseconds get_logid_latency,
    size_t payload_size,
    std::chrono::milliseconds record_timestamp,
    lsn_t from_lsn,
    lsn_t until_lsn,
    const std::function<std::string()>& epoch_metadata_str_factory,
    const std::function<std::string()>& unavailable_nodes_str_factory,
    epoch_t epoch,
    lsn_t trim_point,
    size_t readset_size) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("log_id", std::to_string(logid.val()));
    sample->addIntValue("record_lsn", record_lsn);
    sample->addNormalValue(
        "read_stream_id", std::to_string(read_stream_id.val()));
    sample->addNormalValue("from_node_id", from_node_id.toString());
    sample->addIntValue(
        "epoch_metadata_fetch_latency", epoch_metadata_fetch_latency.count());
    sample->addIntValue("get_logid_latency", get_logid_latency.count());
    sample->addIntValue("payload_size", payload_size);
    sample->addIntValue("record_ts", record_timestamp.count());
    sample->addNormalValue("from_lsn", std::to_string(from_lsn));
    sample->addNormalValue("until_lsn", std::to_string(until_lsn));
    sample->addIntValue("is_tail_follower", until_lsn == LSN_MAX);
    sample->addNormalValue("epoch_metadata", epoch_metadata_str_factory());
    sample->addNormalValue(
        "unavailable_nodes", unavailable_nodes_str_factory());
    sample->addIntValue("epoch", epoch.val());
    sample->addIntValue("trim_point", trim_point);
    sample->addIntValue("readset_size", readset_size);
    return sample;
  };
  publish(READ_TRACER, sample_builder);
}
}} // namespace facebook::logdevice
