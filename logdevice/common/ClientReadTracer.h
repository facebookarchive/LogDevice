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

#include "logdevice/common/SampledTracer.h"

namespace facebook { namespace logdevice {

constexpr auto READ_TRACER = "read_tracer";
class ClientReadTracer : public SampledTracer {
 public:
  explicit ClientReadTracer(std::shared_ptr<TraceLogger> logger);
  void traceRecordDelivery(
      logid_t logid,
      lsn_t record_lsn,
      read_stream_id_t read_stream_id,
      NodeID from_node_id,
      std::chrono::microseconds epoch_metadata_fetch_latency,
      std::chrono::microseconds get_logid_latency,
      size_t payload_size,
      std::chrono::milliseconds record_timestamp,
      // LSN from which the client started reading
      lsn_t from_lsn,
      lsn_t until_lsn,
      // current EpochMetaData
      const std::function<std::string()>& epoch_metadata_str_factory,
      const std::function<std::string()>& unavailable_nodes_str_factory,
      epoch_t epoch,
      lsn_t trim_point,
      size_t readset_size);
};

}} // namespace facebook::logdevice
