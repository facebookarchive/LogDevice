/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/small_vector.h>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class TraceLogger;
struct RebuildingSet;

constexpr auto LOGSCONFIGAPI_TRACER = "logsconfigapi";
constexpr auto LOGSCONFIGAPI_QUERY = "QUERY";
constexpr auto LOGSCONFIGAPI_MUTATION = "MUTATION";

class LogsConfigApiTracer : SampledTracer {
 public:
  explicit LogsConfigApiTracer(std::shared_ptr<TraceLogger> logger)
      : SampledTracer(std::move(logger)) {
    requestReceiveTime_ = std::chrono::steady_clock::now();
  }
  void trace();

  void setRequestType(LOGS_CONFIG_API_Header::Type request_type);
  void setPath(const std::string& path);
  void setClientAddress(std::string client_address);
  void setStatus(Status status);
  void setChunkCount(int chunk_count);
  void setResponseSizeBytes(int response_size_bytes);
  void setDeltaOpType(logsconfig::DeltaOpType delta_op_type);
  void setTreeVersion(lsn_t tree_version);

  LOGS_CONFIG_API_Header::Type requestType_;
  std::string path_;
  std::string clientAddress_;
  Status status_;
  int chunkCount_;
  int responseSizeBytes_;
  logsconfig::DeltaOpType deltaOpType_;
  std::chrono::time_point<std::chrono::steady_clock> requestReceiveTime_;
  lsn_t treeVersion_;
};
}} // namespace facebook::logdevice
