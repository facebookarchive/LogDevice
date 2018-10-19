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

constexpr auto CLIENT_READ_STREAM_TRACER = "client_read_stream_tracer";
class ClientReadStreamTracer : public SampledTracer {
 public:
  enum class Events { REWIND };

  explicit ClientReadStreamTracer(std::shared_ptr<TraceLogger> logger);
  void
  traceEvent(logid_t logid,
             read_stream_id_t read_stream_id,
             // event (eg: rewind)
             Events event,
             // reason/context
             const std::string& details,
             // state
             lsn_t from_lsn,
             lsn_t until_lsn,
             lsn_t last_delivered_lsn,
             std::chrono::milliseconds last_delivered_ts,
             std::chrono::milliseconds last_received_ts,
             const std::function<std::string()>& epoch_metadata_str_factory,
             const std::function<std::string()>& unavailable_nodes_str_factory,
             epoch_t epoch,
             lsn_t trim_point,
             size_t readset_size);
};
}} // namespace facebook::logdevice
