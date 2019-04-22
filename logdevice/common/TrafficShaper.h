/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice {

class Processor;
class StatsHolder;

/**
 * @file A background thread that monitors external events
 *       affecting traffic shaping policy, and periodically adds
 *       bandwidth credit to each Sender's FlowGroups.
 */

class TrafficShaper {
 public:
  explicit TrafficShaper(Processor* processor, StatsHolder* stats = nullptr);
  ~TrafficShaper();

  void shutdown();

  /**
   * Sets the interval at which bandwidth credits are disseminated
   * to Senders.
   */
  template <class Rep, class Period>
  void setInterval(const std::chrono::duration<Rep, Period>& interval) {
    setIntervalImpl(
        std::chrono::duration_cast<decltype(updateInterval_)>(interval));
  }

 private:
  class RunFlowGroupsRequest;
  class RunFlowGroupsRequestReadIO;

  Processor* processor_;
  StatsHolder* stats_;
  std::thread mainLoopThread_;

  std::atomic<bool> mainLoopRunning_{false};
  std::atomic<bool> mainLoopStop_{false};
  std::condition_variable mainLoopWaitCondition_;
  std::mutex mainLoopWaitMutex_;

  std::chrono::microseconds updateInterval_{1000};
  std::unique_ptr<FlowGroupsUpdate> nw_update_;
  std::unique_ptr<FlowGroupsUpdate> read_io_update_;

  // comes last to ensure unsubscription before rest of destruction
  ConfigSubscriptionHandle config_update_sub_;

  std::chrono::seconds limits_update_interval_{30};
  std::chrono::time_point<std::chrono::steady_clock> next_limits_publication_ =
      std::chrono::time_point<std::chrono::steady_clock>();

  /**
   * Receiver of configuration change unotifications.
   */
  static void onConfigUpdate(TrafficShaper*);

  /**
   * Common method to update FlowGroupsUpdate related stats.
   */
  void updateStats(const configuration::ShapingConfig* shaping_cfg,
                   FlowGroupDependencies* deps);

  /**
   * Repeatedly sleeps for updateInterval_ then distributes credit.
   */
  void mainLoop();

  /**
   * Construct and release a FlowGroupsUpdate to each worker.
   *
   * @return true   Periodic FlowGroupsUpdates should continue.
   *         false  All FlowGroups are disabled, so the TrafficShaping
   *                thread can sleep until a configuration change occurs.
   */
  bool dispatchUpdateNw();
  bool dispatchUpdateReadIO();
  bool dispatchUpdateCommon(const configuration::ShapingConfig& shaping_config,
                            int nworkers,
                            FlowGroupsUpdate& update,
                            FlowGroupDependencies* deps);

  void setIntervalImpl(const decltype(updateInterval_)& interval);
  std::unique_ptr<FlowGroupDependencies> nw_shaping_deps_;
  std::unique_ptr<FlowGroupDependencies> read_shaping_deps_;
};

}} // namespace facebook::logdevice
