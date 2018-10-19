/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientStalledReadTracer.h"

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

ClientStalledReadTracer::ClientStalledReadTracer(
    std::shared_ptr<TraceLogger> logger)
    : ThrottledTracer(std::move(logger),
                      STALL_READ_TRACER,
                      std::chrono::seconds(10),
                      10) {}

void ClientStalledReadTracer::traceStall(
    logid_t logid,
    read_stream_id_t read_stream_id,
    lsn_t from_lsn,
    lsn_t until_lsn,
    lsn_t last_delivered_lsn,
    std::chrono::milliseconds last_delivered_ts,
    std::chrono::milliseconds last_received_ts,
    const std::string& reason,
    const std::function<std::string()>& epoch_metadata_str_factory,
    const std::function<std::string()>& unavailable_nodes_str_factory,
    epoch_t epoch,
    lsn_t trim_point,
    size_t readset_size) {
  auto sample_builder = [=]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("log_id", std::to_string(logid.val()));
    sample->addNormalValue(
        "read_stream_id", std::to_string(read_stream_id.val()));
    sample->addNormalValue("from_lsn", std::to_string(from_lsn));
    sample->addNormalValue("until_lsn", std::to_string(until_lsn));

    sample->addIntValue("last_delivered_lsn", last_delivered_lsn);
    sample->addIntValue("last_delivered_ts", last_delivered_ts.count());
    sample->addIntValue("last_received_ts", last_received_ts.count());
    sample->addNormalValue("reason", reason);

    sample->addNormalValue("epoch_metadata", epoch_metadata_str_factory());
    sample->addNormalValue(
        "unavailable_nodes", unavailable_nodes_str_factory());
    sample->addIntValue("epoch", epoch.val());
    sample->addIntValue("trim_point", trim_point);
    sample->addIntValue("readset_size", readset_size);
    return sample;
  };
  publish(sample_builder);
}
}} // namespace facebook::logdevice
