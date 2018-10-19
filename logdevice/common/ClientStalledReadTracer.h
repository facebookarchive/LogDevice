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

#include "logdevice/common/ThrottledTracer.h"

namespace facebook { namespace logdevice {

constexpr auto STALL_READ_TRACER = "stall_read_tracer";
class ClientStalledReadTracer : public ThrottledTracer {
 public:
  explicit ClientStalledReadTracer(std::shared_ptr<TraceLogger> logger);
  void
  traceStall(logid_t logid,
             read_stream_id_t read_stream_id,
             // LSN from which the client started reading
             lsn_t from_lsn,
             lsn_t until_lsn,
             lsn_t last_delivered_lsn,
             std::chrono::milliseconds last_delivered_ts,
             std::chrono::milliseconds last_received_ts,
             const std::string& reason,
             // current EpochMetaData
             const std::function<std::string()>& epoch_metadata_str_factory,
             const std::function<std::string()>& unavailable_nodes_str_factory,
             epoch_t epoch,
             lsn_t trim_point,
             size_t readset_size);
};
}} // namespace facebook::logdevice
