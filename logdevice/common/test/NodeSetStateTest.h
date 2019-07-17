/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/NodeSetState.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)

namespace facebook { namespace logdevice {

class NodeSetStateTest : public ::testing::Test {
 public:
  const logid_t LOG_ID{2};
  StorageSet storage_set_{N0, N1, N2, N3};
};

class MyNodeSetState : public NodeSetState {
 public:
  MyNodeSetState(const StorageSet& shards,
                 logid_t log_id,
                 HealthCheck healthCheck,
                 std::shared_ptr<Configuration> config = nullptr)
      : NodeSetState(shards, log_id, healthCheck) {
    config_ = config;
  }

  void postHealthCheckRequest(ShardID /* unused */,
                              bool /* unused */) override {}

  std::chrono::steady_clock::time_point
  getDeadline(NotAvailableReason /* unused */) const override {
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  }

  double getSpaceBasedRetentionThreshold() const override {
    return 0;
  }
  bool shouldPerformSpaceBasedRetention() const override {
    return true;
  }

  double getGrayListThreshold() const override {
    return 0.5;
  }

  const std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  const Settings* getSettings() const override {
    return nullptr;
  }

 private:
  std::shared_ptr<Configuration> config_;
};

}} // namespace facebook::logdevice
