/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <vector>

#include <folly/Random.h>
#include <folly/small_vector.h>

#include "event2/event.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/TrafficShapingConfig.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/ServerHistograms.h"

/**
 * @file ShapingContainer is a class that captures common functionality
 * between Network Traffic Shaping and Read Throttling.
 * It holds FlowGroups which are used to throttle bandwidth(network or
 * read I/O)
 * Sender(on each worker) instantiates this class for n/w traffic shaping.
 * For read throttling, this can be done on a Worker.
 */

namespace facebook { namespace logdevice {
class ShapingContainer {
 public:
  enum class RunType { REPLENISH, EVENTLOOP };
  explicit ShapingContainer(size_t num_scopes,
                            struct event_base* base,
                            const configuration::ShapingConfig* sc,
                            FlowGroupType type)
      : type_(type),
        num_scopes_(num_scopes),
        flow_groups_run_requested_(LD_EV(event_new)(
            base,
            -1,
            EV_WRITE | EV_PERSIST,
            EventHandler<ShapingContainer::onFlowGroupsRunRequested>,
            this)),
        flow_groups_run_deadline_exceeded_(LD_EV(event_new)(
            base,
            -1,
            0,
            EventHandler<ShapingContainer::onFlowGroupsRunRequested>,
            this)) {
    flow_groups_.resize(num_scopes);

    auto scope = NodeLocationScope::NODE;
    for (auto& fg : flow_groups_) {
      fg.configure(sc->configured(scope));
      scope = NodeLocation::nextGreaterScope(scope);
      fg.setType(type);
    }

    if (!flow_groups_run_requested_) { // unlikely
      ld_error("Failed to create 'flow groups run requested' event");
      err = E::NOMEM;
      throw ConstructorFailed();
    }

    LD_EV(event_priority_set)
    (flow_groups_run_requested_, EventLoop::PRIORITY_LOW);

    if (!flow_groups_run_deadline_exceeded_) { // unlikely
      ld_error("Failed to create 'flow groups run deadline exceeded' event");
      err = E::NOMEM;
      throw ConstructorFailed();
    }
  }

  ~ShapingContainer() {
    LD_EV(event_free)(flow_groups_run_requested_);
    LD_EV(event_free)(flow_groups_run_deadline_exceeded_);
  }

  /**
   * Apply a policy update and bandwidth credit increment to all
   * FlowGroups.
   *
   * @return true if an update makes a FlowGroup runnable.
   */
  bool applyFlowGroupsUpdate(FlowGroupsUpdate& update, StatsHolder* stats) {
    std::unique_lock<std::mutex> lock(flow_meters_mutex_);
    bool run = false;

    for (size_t i = 0; i < flow_groups_.size(); ++i) {
      if (flow_groups_[i].applyUpdate(update.group_entries[i], stats)) {
        run = true;
      }
    }

    return run;
  }

  void updateFlowGroupRunRequestedTime(SteadyTimestamp enqueue_time) {
    flow_groups_run_requested_time_ = enqueue_time;
  }

  /**
   * Dispatch messages from all flow groups until they either are
   * empty or have exhausted their bandwidth credit.
   */
  void runFlowGroups(RunType /*rt*/) {
    LD_EV(event_del)(flow_groups_run_requested_);
    evtimer_del(flow_groups_run_deadline_exceeded_);

    if (flow_groups_run_requested_time_ != SteadyTimestamp()) {
      auto queue_latency =
          std::chrono::duration_cast<std::chrono::microseconds>(
              SteadyTimestamp::now() - flow_groups_run_requested_time_);
      HISTOGRAM_ADD(Worker::stats(),
                    flow_groups_run_event_loop_delay,
                    queue_latency.count());
      flow_groups_run_requested_time_ = SteadyTimestamp();
    }

    auto run_start_time = SteadyTimestamp::now();
    auto run_deadline =
        run_start_time + Worker::settings().flow_groups_run_yield_interval;
    bool exceeded_deadline = false;

    // Shuffle FlowGroups so that all get a chance to run even if
    // only a subset take the majority of the allowed runtime.
    folly::small_vector<int, NodeLocation::NUM_ALL_SCOPES> fg_ids(
        flow_groups_.size());
    fg_ids.resize(num_scopes_);
    std::iota(fg_ids.begin(), fg_ids.end(), 0);
    std::shuffle(fg_ids.begin(), fg_ids.end(), folly::ThreadLocalPRNG());
    for (auto idx : fg_ids) {
      exceeded_deadline =
          flow_groups_[idx].run(flow_meters_mutex_, run_deadline);
      if (exceeded_deadline) {
        // Run again after yielding to the event loop.
        STAT_INCR(Worker::stats(), flow_groups_run_deadline_exceeded);
        flow_groups_run_requested_time_ = SteadyTimestamp::now();
        evtimer_add(flow_groups_run_deadline_exceeded_,
                    Worker::onThisThread()->zero_timeout_);
        break;
      }
    }

    HISTOGRAM_ADD(Worker::stats(),
                  flow_groups_run_time,
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      SteadyTimestamp::now() - run_start_time)
                      .count());
  }

  static void onFlowGroupsRunRequested(void* arg, short) {
    ShapingContainer* self = reinterpret_cast<ShapingContainer*>(arg);
    self->runFlowGroups(RunType::EVENTLOOP);
  }

  FlowGroup& selectFlowGroup(NodeLocationScope starting_scope) {
    // Search for a configured FlowGroup with the smallest scope.
    // Note: Scope NODE and ROOT are always configured (defaulting to
    //       disabled -- i.e. no limits). So this search will always
    //       succeed even if in a cluster without any FlowGroups
    //       enforcing a policy.
    while (starting_scope < NodeLocationScope::ROOT &&
           !flow_groups_[static_cast<int>(starting_scope)].configured()) {
      starting_scope = NodeLocation::nextGreaterScope(starting_scope);
    }
    return flow_groups_[static_cast<int>(starting_scope)];
  }

  FlowGroupType type_{FlowGroupType::NONE};
  std::vector<FlowGroup> flow_groups_;
  // Provides mutual exclusion between application of flow group updates
  // by the TrafficShaper thread and normal packet transmission on this
  // Sender.
  //
  // Note: Flow group updates only modify the FlowMeters within FlowGroups
  //       and perform thread safe tests to see if FlowGroups need to
  //       be run. For this reason, the flow_meters_mutex_ does not
  //       need to be held during operations that remove elements from
  //       a FlowGroup's priority queue. Operations such as trim or
  //       the cleanup of queued messages when a Socket is closed take
  //       advantage of this property to avoid having to reach up into
  //       the Sender to acquire this lock which, in many error paths,
  //       is already held.
  std::mutex flow_meters_mutex_;

  size_t num_scopes_;

  // Event signalled when there is demand for priority queue bandwidth.
  // When activated, this low priority event will be serviced once the
  // event loop goes idle for normal priority events. This allows demand
  // from multiple priorities to be aggregated before servicing.
  struct event* flow_groups_run_requested_;

  // Backup event for flow_groups_run_requested_ when the Worker is saturated.
  // When this timer event fires, the flow group run is scheduled at normal
  // priority and is serviced in FIFO order with all other events.
  struct event* flow_groups_run_deadline_exceeded_;

  SteadyTimestamp flow_groups_run_requested_time_;
};

}} // namespace facebook::logdevice
