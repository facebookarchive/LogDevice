/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TrafficShaper.h"

#include <algorithm>
#include <numeric>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

class TrafficShaper::RunFlowGroupsRequest : public Request {
 public:
  RunFlowGroupsRequest()
      : Request(RequestType::TRAFFIC_SHAPER_RUN_FLOW_GROUPS) {}
  Execution execute() override {
    auto w = Worker::onThisThread();
    w->sender().getNwShapingContainer()->updateFlowGroupRunRequestedTime(
        enqueue_time_);
    w->sender().getNwShapingContainer()->runFlowGroups(
        ShapingContainer::RunType::REPLENISH);
    return Request::Execution::COMPLETE;
  }
};

TrafficShaper::TrafficShaper(Processor* processor, StatsHolder* stats)
    : processor_(processor), stats_(stats) {
  nw_update_ = std::make_unique<FlowGroupsUpdate>(
      static_cast<size_t>(NodeLocationScope::ROOT) + 1);

  if (processor_->getAllWorkersCount() != 0) {
    mainLoopThread_ = std::thread(&TrafficShaper::mainLoop, this);

    mainLoopRunning_.store(true);
    config_update_sub_ =
        processor->config_->updateableServerConfig()->subscribeToUpdates(
            std::bind(&TrafficShaper::onConfigUpdate, this));
  }
}

TrafficShaper::~TrafficShaper() {
  shutdown();
}

void TrafficShaper::shutdown() {
  mainLoopStop_.store(true);
  if (mainLoopRunning_.exchange(false)) {
    std::unique_lock<std::mutex> cv_lock(mainLoopWaitMutex_);
    mainLoopWaitCondition_.notify_all();
    cv_lock.unlock();
    mainLoopThread_.join();
  }
}

void TrafficShaper::onConfigUpdate(TrafficShaper* ts) {
  // Ensure the TrafficShaper is idle by acquiring the lock.
  // By waiting until the TrafficShaper is idle, we know that either the
  // TrafficShaper is already awake, blocked on this mutex, and about to
  // read the new config anyway, or will be woken by this notification
  // and see the new config. Without this synchronization, it is possible
  // for the TrafficShaper to go to sleep (potentially forever) just
  // after we notify it, having never seen the update.
  std::unique_lock<std::mutex> cv_lock(ts->mainLoopWaitMutex_);
  ts->mainLoopWaitCondition_.notify_all();
}

void TrafficShaper::mainLoop() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:tshaper");

  auto next_run = std::chrono::steady_clock::now();
  std::unique_lock<std::mutex> cv_lock(mainLoopWaitMutex_);
  while (!mainLoopStop_.load()) {
    HISTOGRAM_ADD(stats_, traffic_shaper_bw_dispatch, usec_since(next_run));
    bool timed_sleep = dispatchUpdateNw();
    auto now = std::chrono::steady_clock::now();
    next_run += updateInterval_;
    if (now < next_run) {
      if (timed_sleep) {
        mainLoopWaitCondition_.wait_for(cv_lock, next_run - now);
      } else {
        mainLoopWaitCondition_.wait(cv_lock);
        // Don't record a long dispatch delay since we purposely slept
        // for an indeterminate amount of time (e.g. until a configuration
        // change occurred).
        next_run = std::chrono::steady_clock::now();
      }
    } else {
      // We are running late. Dispatch the next quantum immediately.
      // next_run isn't updated since we want any bandwidth credits we
      // should have accumulated to be distributed to Senders. The
      // maximum burst setting should still prevent network overloading.
    }

    if (now > next_limits_publication_) {
      auto config = processor_->config_->updateableServerConfig()->get();
      const auto& shaping_config = config->getTrafficShapingConfig();

      auto scope = NodeLocationScope::NODE;
      for (auto& policy : shaping_config.flowGroupPolicies) {
        auto value = 0;
        Priority p = Priority::MAX;
        for (const auto& entry : policy.second.entries) {
          value += entry.guaranteed_bw;

          if (p == Priority::INVALID) {
            continue;
          }

          if (entry.max_bw != INT64_MAX) {
            FLOW_GROUP_PRIORITY_STAT_SET(
                stats_, scope, p, max_bw, entry.max_bw);
          }

          FLOW_GROUP_PRIORITY_STAT_SET(
              stats_, scope, p, guaranteed_bw, entry.guaranteed_bw);

          FLOW_GROUP_PRIORITY_STAT_SET(
              stats_, scope, p, max_burst, entry.capacity);

          p = priorityBelow(p);
        }

        FLOW_GROUP_STAT_SET(stats_, scope, limit, value);

        scope = NodeLocation::nextGreaterScope(scope);
      }
      next_limits_publication_ = now + limits_update_interval_;
    }
  }
}

bool TrafficShaper::dispatchUpdateCommon(
    const configuration::ShapingConfig& shaping_config,
    int nworkers,
    FlowGroupsUpdate& update,
    StatsHolder* stats) {
  bool future_updates_required = false;
  auto policy_it = shaping_config.flowGroupPolicies.begin();
  auto scope = NodeLocationScope::NODE;
  for (auto& ge : update.group_entries) {
    if (policy_it->second.enabled()) {
      future_updates_required = true;
    }

    // The policy or interval may change at any time via the admin
    // interface, so normalize on each update.
    ge.policy = policy_it->second.normalize(nworkers, updateInterval_);

    // Any overflow from the last run that couldn't be used in the
    // priority queue buckets indicates that the priority queues have
    // filled to capacity.  To ensure compliance with the maximum burst
    // setting, discard this credit so it isn't applied on the next run.
    auto& pq_overflow_entry = ge.priorityQEntry();
    pq_overflow_entry.last_overflow = 0;

    Priority p = Priority::MAX;

    for (auto& overflow_entry : ge.overflow_entries) {
      // Excess bandwidth that couldn't be consumed during a second
      // first-fit pass on the buckets for a priority level across
      // all workers is donated to the priority queue.
      pq_overflow_entry.cur_overflow += overflow_entry.last_overflow;

      if (p <= Priority::NUM_PRIORITIES) {
        FLOW_GROUP_PRIORITY_STAT_ADD(
            stats, scope, p, bwdiscarded, overflow_entry.last_overflow);
      }

      // Take the current overflow and make it available for next
      // run's first fit pass.
      overflow_entry.last_overflow = overflow_entry.cur_overflow;
      overflow_entry.cur_overflow = 0;
      p = priorityBelow(p);
    }

    ++policy_it;
    scope = NodeLocationScope(static_cast<int>(scope) + 1);
  }

  return future_updates_required;
}

bool TrafficShaper::dispatchUpdateNw() {
  auto config = processor_->config_->updateableServerConfig()->get();
  const configuration::ShapingConfig& shaping_config =
      config->getTrafficShapingConfig();
  bool future_updates_required = dispatchUpdateCommon(
      shaping_config, processor_->getAllWorkersCount(), *nw_update_, stats_);

  processor_->applyToWorkers(
      [&](Worker& w) {
        if (w.sender().getNwShapingContainer()->applyFlowGroupsUpdate(
                *nw_update_, stats_)) {
          std::unique_ptr<Request> run_req =
              std::make_unique<RunFlowGroupsRequest>();
          processor_->postRequest(run_req, w.worker_type_, w.idx_.val());
        }
      },
      Processor::Order::RANDOM);

  return future_updates_required;
}

void TrafficShaper::setIntervalImpl(const decltype(updateInterval_)& interval) {
  std::unique_lock<std::mutex> cv_lock(mainLoopWaitMutex_);
  updateInterval_ = interval;
}

}} // namespace facebook::logdevice
