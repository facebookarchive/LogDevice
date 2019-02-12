/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigApiTracer.h"

#include <algorithm>
#include <memory>
#include <string>

#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"

namespace facebook { namespace logdevice {

void LogsConfigApiTracer::setRequestType(
    LOGS_CONFIG_API_Header::Type request_type) {
  requestType_ = request_type;
}

void LogsConfigApiTracer::setPath(const std::string& path) {
  path_ = path;
}

void LogsConfigApiTracer::setClientAddress(std::string client_address) {
  clientAddress_ = client_address;
}

void LogsConfigApiTracer::setStatus(Status status) {
  status_ = status;
}

void LogsConfigApiTracer::setChunkCount(int chunk_count) {
  chunkCount_ = chunk_count;
}

void LogsConfigApiTracer::setResponseSizeBytes(int response_size_bytes) {
  responseSizeBytes_ = response_size_bytes;
}

void LogsConfigApiTracer::setDeltaOpType(
    logsconfig::DeltaOpType delta_op_type) {
  deltaOpType_ = delta_op_type;
}

void LogsConfigApiTracer::setTreeVersion(lsn_t tree_version) {
  treeVersion_ = tree_version;
}

std::string stringifyHeaderType(LOGS_CONFIG_API_Header::Type request_type,
                                logsconfig::DeltaOpType delta_op_type) {
  std::string request_name;

  switch (request_type) {
    case LOGS_CONFIG_API_Header::Type::GET_DIRECTORY:
      request_name = "GET_DIRECTORY";
      break;
    case LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_NAME:
      request_name = "GET_LOG_GROUP_BY_NAME"; // used to be GET_LOG_GROUP
      break;
    case LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID:
      request_name = "GET_LOG_GROUP_BY_ID";
      break;
    case LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST:
      return logsconfig::deltaOpTypeName(delta_op_type);
      break;
    default:
      ld_warning("Tracing an unknwown request type %u",
                 static_cast<uint>(request_type));
      request_name = "UNKNOWN";
  }
  return request_name;
}

void LogsConfigApiTracer::trace() {
  auto sample_builder = [&]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();

    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - requestReceiveTime_);
    sample->addNormalValue(
        "request_type", stringifyHeaderType(requestType_, deltaOpType_));
    sample->addNormalValue(
        "is_mutation",
        requestType_ == LOGS_CONFIG_API_Header::Type::MUTATION_REQUEST
            ? "true"
            : "false");
    sample->addNormalValue("status", error_name(status_));
    sample->addNormalValue("client_address", clientAddress_);
    sample->addNormalValue("path", path_);
    sample->addIntValue("latency_ms", latency.count());
    sample->addIntValue("logsconfig_tree_version", treeVersion_);
    sample->addIntValue("chunck_count", chunkCount_);
    sample->addIntValue("response_size_bytes", responseSizeBytes_);
    return sample;
  };
  publish(LOGSCONFIGAPI_TRACER, sample_builder);
}
}} // namespace facebook::logdevice
