/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

/**
 * @file LazySequencerPlacement relies on APPEND message handling to bring up
 *       a Sequencer as needed (with help by FailureDetector).
 */

class LazySequencerPlacement : public SequencerPlacement {
 public:
  LazySequencerPlacement(Processor* processor,
                         UpdateableSettings<GossipSettings> settings)
      : failure_detector_(checked_downcast<ServerProcessor*>(processor)
                              ->failure_detector_.get()),
        settings_(std::move(settings)) {
    if (failure_detector_ == nullptr) {
      ld_error("Failure detector not initialized");
      throw ConstructorFailed();
    }
  }

  void requestFailover() override {
    // Initiate shard failover and allow the request to be propagated to other
    // nodes. In the meantime, this node can still process appends that get sent
    // its way.
    failure_detector_->failover();
    auto wait_time = settings_->failover_wait_time;
    if (wait_time.count() > 0) {
      ld_info("Failover initiated, waiting %lu ms", wait_time.count());
      /* sleep override */
      std::this_thread::sleep_for(wait_time);
    }
  }

 private:
  FailureDetector* failure_detector_;
  UpdateableSettings<GossipSettings> settings_;
};

}} // namespace facebook::logdevice
