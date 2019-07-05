/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <functional>

#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class MockStorageSetAccessor : public StorageSetAccessor {
 public:
  MockStorageSetAccessor(
      logid_t log_id,
      StorageSet storage_set,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      ReplicationProperty replication,
      ShardAccessFunc shard_access,
      CompletionFunc completion,
      Property property,
      std::chrono::milliseconds timeout = std::chrono::milliseconds::zero())
      : StorageSetAccessor(log_id,
                           storage_set,
                           nodes_configuration,
                           replication,
                           shard_access,
                           completion,
                           property,
                           timeout) {}

  std::unique_ptr<Timer> createJobTimer(std::function<void()>) override {
    return nullptr;
  }

  std::unique_ptr<Timer>
  createGracePeriodTimer(std::function<void()>) override {
    return nullptr;
  }

  void cancelJobTimer() override {
    job_timer_active_ = false;
  }
  void activateJobTimer() override {
    job_timer_active_ = true;
  }
  void cancelGracePeriodTimer() override {
    grace_period_timer_active_ = false;
  }
  void activateGracePeriodTimer() override {
    grace_period_timer_active_ = true;
  }
  void setGracePeriod(std::chrono::milliseconds grace_period,
                      CompletionCondition completion_cond) override {
    ld_check(!started_);
    grace_period_ = grace_period;
    completion_cond_ = folly::Optional<CompletionCondition>(completion_cond);
  }
  bool isJobTimerActive() {
    return job_timer_active_;
  }
  bool isGracePeriodTimerActive() {
    return grace_period_timer_active_;
  }
  void mockJobTimeout() {
    onJobTimedout();
  }
  void mockGracePeriodTimedout() {
    onGracePeriodTimedout();
  }

  std::unique_ptr<BackoffTimer>
  createWaveTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    return std::move(timer);
  }

  std::unique_ptr<CopySetSelector> createCopySetSelector(
      logid_t,
      const EpochMetaData&,
      std::shared_ptr<NodeSetState>,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&)
      override {
    return nullptr;
  }

  void setInitialShardAuthStatusMap(ShardAuthoritativeStatusMap map) {
    ld_check(!started_);
    map_ = map;
  }

  void mockShardStatusChanged(ShardID shard, AuthoritativeStatus auth_st) {
    map_.setShardStatus(shard.node(), shard.shard(), auth_st);
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return map_;
  }

 private:
  bool job_timer_active_{false};
  bool grace_period_timer_active_{false};
  ShardAuthoritativeStatusMap map_;
};

}} // namespace facebook::logdevice
