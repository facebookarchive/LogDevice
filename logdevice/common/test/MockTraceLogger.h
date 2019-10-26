/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {

class MockTraceLogger : public TraceLogger {
 public:
  explicit MockTraceLogger(
      const std::shared_ptr<UpdateableConfig> cluster_config,
      const folly::Optional<NodeID>& my_node_id = folly::none)
      : TraceLogger(cluster_config, my_node_id) {}

  void pushSample(const std::string& table,
                  int32_t /* sample_rate */,
                  std::unique_ptr<TraceSample> sample) override {
    pushed_samples[table].push_back(std::move(sample));
  }

  // A map from table name to the list of pushed samples
  std::unordered_map<std::string, std::vector<std::unique_ptr<TraceSample>>>
      pushed_samples;
};

}} // namespace facebook::logdevice
