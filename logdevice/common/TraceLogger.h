/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/TraceSample.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"

namespace facebook { namespace logdevice {

/**
 * TraceLogger is a virtual interface abstracting a sink for all trace samples.
 * The logger is responsible for transcoding (if needed) and publishing (usually
 * async) this to a tracing backend.
 */
class TraceLogger {
 public:
  explicit TraceLogger(const std::shared_ptr<UpdateableConfig> cluster_config,
                       const folly::Optional<NodeID>& my_node_id)
      : cluster_config_(cluster_config), my_node_id_(my_node_id){};

  // Thread safe.
  virtual void pushSample(const std::string& table,
                          int32_t sample_rate,
                          std::unique_ptr<TraceSample> sample) = 0;
  virtual ~TraceLogger() {}

  folly::Optional<double>
  getSamplePercentageForTracer(const std::string& tracer) const {
    return cluster_config_->get()->serverConfig()->getTracerSamplePercentage(
        tracer);
  }

  double getDefaultSamplePercentage() const {
    return cluster_config_->get()->serverConfig()->getDefaultSamplePercentage();
  }

  /** Helpers useful in tracing **/
  std::string nodeIDToIPAddress(node_index_t idx) const {
    const auto& node =
        cluster_config_->getNodesConfiguration()->getNodeServiceDiscovery(idx);
    if (node == nullptr) {
      return std::string();
    }
    return node->address.toStringNoBrackets();
  }

  std::shared_ptr<Configuration> getConfiguration() const {
    return cluster_config_->get();
  }

 protected:
  const std::shared_ptr<UpdateableConfig> cluster_config_;
  const folly::Optional<NodeID> my_node_id_;
};
}} // namespace facebook::logdevice
