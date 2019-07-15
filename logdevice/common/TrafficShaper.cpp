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
  RunFlowGroupsRequest(ShapingContainer* container, RequestType type)
      : Request(type), container_(container) {}
  Execution execute() override {
    container_->updateFlowGroupRunRequestedTime(enqueue_time_);
    container_->runFlowGroups(ShapingContainer::RunType::REPLENISH);
    return Request::Execution::COMPLETE;
  }

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

 private:
  ShapingContainer* container_{nullptr};
};

TrafficShaper::TrafficShaper(Processor* processor, StatsHolder* stats)
    : processor_(processor), stats_(stats) {
  nw_update_ = std::make_unique<FlowGroupsUpdate>(
      std::set<NodeLocationScope>({NodeLocationScope::NODE,
                                   NodeLocationScope::RACK,
                                   NodeLocationScope::ROW,
                                   NodeLocationScope::CLUSTER,
                                   NodeLocationScope::DATA_CENTER,
                                   NodeLocationScope::REGION,
                                   NodeLocationScope::ROOT}));
  read_io_update_ = std::make_unique<FlowGroupsUpdate>(
      std::set<NodeLocationScope>({NodeLocationScope::NODE}));

  nw_shaping_deps_ = std::make_unique<NwShapingFlowGroupDeps>(stats);
  read_shaping_deps_ = std::make_unique<ReadShapingFlowGroupDeps>(stats);

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

void TrafficShaper::updateStats(const configuration::ShapingConfig* shaping_cfg,
                                FlowGroupDependencies* deps) {
  for (auto& policy : shaping_cfg->flowGroupPolicies) {
    auto value = 0;
    auto& scope = policy.first;
    Priority p = Priority::MAX;
    for (const auto& entry : policy.second.entries) {
      value += entry.guaranteed_bw;

      if (p == Priority::INVALID) {
        continue;
      }

      if (entry.max_bw != INT64_MAX) {
        deps->statsSet(
            &PerShapingPriorityStats::max_bw, scope, p, entry.max_bw);
      }
      deps->statsSet(&PerShapingPriorityStats::guaranteed_bw,
                     scope,
                     p,
                     entry.guaranteed_bw);
      deps->statsSet(
          &PerShapingPriorityStats::max_burst, scope, p, entry.capacity);

      p = priorityBelow(p);
    }

    deps->statsSet(&PerFlowGroupStats::limit, scope, value);
  }
}

void TrafficShaper::mainLoop() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:tshaper");

  auto next_run = std::chrono::steady_clock::now();
  std::unique_lock<std::mutex> cv_lock(mainLoopWaitMutex_);
  while (!mainLoopStop_.load()) {
    HISTOGRAM_ADD(stats_, traffic_shaper_bw_dispatch, usec_since(next_run));
    bool timed_sleep_nw = dispatchUpdateNw();
    bool timed_sleep_readio = dispatchUpdateReadIO();
    auto now = std::chrono::steady_clock::now();
    next_run += updateInterval_;
    if (now < next_run) {
      if (timed_sleep_nw || timed_sleep_readio) {
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
      updateStats(&config->getTrafficShapingConfig(), nw_shaping_deps_.get());
      updateStats(&config->getReadIOShapingConfig(), read_shaping_deps_.get());
      next_limits_publication_ = now + limits_update_interval_;
    }
  }
}

bool TrafficShaper::dispatchUpdateCommon(
    const configuration::ShapingConfig& shaping_config,
    int nworkers,
    FlowGroupsUpdate& update,
    FlowGroupDependencies* deps) {
  bool future_updates_required = false;
  for (auto& policy_it : shaping_config.flowGroupPolicies) {
    auto scope = policy_it.first;
    auto entry_it = update.group_entries.find(scope);
    if (entry_it == update.group_entries.end()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      2,
                      "Didn't find scope:%d in group_entries",
                      static_cast<int>(scope));
      continue;
    }
    auto& ge = entry_it->second;
    if (policy_it.second.enabled()) {
      future_updates_required = true;
    }

    // The policy or interval may change at any time via the admin
    // interface, so normalize on each update.
    ge.policy = policy_it.second.normalize(nworkers, updateInterval_);

    // Any overflow from the last run that couldn't be used in the
    // priority queue buckets indicates that the priority queues have
    // filled to capacity.  To ensure compliance with the maximum burst
    // setting, discard this credit so it isn't applied on the next run.
    auto& pq_overflow_entry = ge.priorityQEntry();
    pq_overflow_entry.last_overflow = 0;

    Priority p = Priority::MAX;

    // This loop takes extra bandwidth from each Prioirty Level and
    // gives it to the priority queue overflow bucket
    for (auto& overflow_entry : ge.overflow_entries) {
      // Excess bandwidth that couldn't be consumed during a second
      // first-fit pass on the buckets for a priority level across
      // all workers is donated to the priority queue.
      pq_overflow_entry.cur_overflow += overflow_entry.last_overflow;

      if (p <= Priority::NUM_PRIORITIES) {
        deps->statsAdd(&PerShapingPriorityStats::bwdiscarded,
                       scope,
                       p,
                       overflow_entry.last_overflow);
      }

      // Take the current overflow and make it available for next
      // run's first fit pass.
      overflow_entry.last_overflow = overflow_entry.cur_overflow;
      overflow_entry.cur_overflow = 0;
      p = priorityBelow(p);
    }
  }

  return future_updates_required;
}

bool TrafficShaper::dispatchUpdateNw() {
  auto config = processor_->config_->updateableServerConfig()->get();
  const configuration::ShapingConfig& shaping_config =
      config->getTrafficShapingConfig();
  bool future_updates_required =
      dispatchUpdateCommon(shaping_config,
                           processor_->getAllWorkersCount(),
                           *nw_update_,
                           nw_shaping_deps_.get());

  processor_->applyToWorkers(
      [&](Worker& w) {
        if (w.sender().getNwShapingContainer()->applyFlowGroupsUpdate(
                *nw_update_, stats_)) {
          std::unique_ptr<Request> run_req =
              std::make_unique<RunFlowGroupsRequest>(
                  w.sender().getNwShapingContainer(),
                  RequestType::TRAFFIC_SHAPER_RUN_FLOW_GROUPS);
          processor_->postImportant(run_req, w.worker_type_, w.idx_.val());
        }
      },
      Processor::Order::RANDOM);

  return future_updates_required;
}

bool TrafficShaper::dispatchUpdateReadIO() {
  auto config = processor_->config_->updateableServerConfig()->get();
  const configuration::ShapingConfig& shaping_config =
      config->getReadIOShapingConfig();
  bool future_updates_required =
      dispatchUpdateCommon(shaping_config,
                           processor_->getWorkerCount(WorkerType::GENERAL),
                           *read_io_update_,
                           read_shaping_deps_.get());

  processor_->applyToWorkers(
      [&](Worker& w) {
        if (w.readShapingContainer().applyFlowGroupsUpdate(
                *read_io_update_, nullptr /*stats*/)) {
          std::unique_ptr<Request> run_req =
              std::make_unique<RunFlowGroupsRequest>(
                  &w.readShapingContainer(),
                  RequestType::READIO_SHAPER_RUN_FLOW_GROUPS);
          processor_->postImportant(run_req, w.worker_type_, w.idx_.val());
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
