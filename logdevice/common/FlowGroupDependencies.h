/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * External dependencies of FlowGroup are isolated into this class for
 * the ease of unit testing.
 */
class FlowGroupDependencies {
 public:
  FlowGroupDependencies() {}

  // Using C++ "pointer to member" feature to provide a common method
  // for updating FlowGroup and Priority related stats by name.
  // Caller will have to do the following:
  // deps->statsSet(&ClassName::stat_field, scope, value)
  void statsSet(StatsCounter PerFlowGroupStats::*stat_field,
                NodeLocationScope scope,
                size_t val) {
    auto p_fgp_stats = statsGet(scope);
    if (!p_fgp_stats) {
      return;
    }
    p_fgp_stats->*stat_field = val;
  }

  void statsSet(StatsCounter PerShapingPriorityStats::*stat_field,
                NodeLocationScope scope,
                Priority p,
                size_t val) {
    auto pri_ptr = getShapingPriorityStats(scope, p);
    if (!pri_ptr) {
      return;
    }
    pri_ptr->*stat_field = val;
  }

  void statsAdd(StatsCounter PerShapingPriorityStats::*stat_field,
                NodeLocationScope scope,
                Priority p,
                size_t val) {
    auto pri_ptr = getShapingPriorityStats(scope, p);
    if (!pri_ptr) {
      return;
    }
    pri_ptr->*stat_field += val;
  }

  PerShapingPriorityStats* getShapingPriorityStats(NodeLocationScope scope,
                                                   Priority p) {
    auto p_fgp_stats = statsGet(scope);
    if (!p_fgp_stats) {
      return nullptr;
    }
    return &p_fgp_stats->priorities[asInt(p)];
  }

  virtual PerFlowGroupStats* statsGet(NodeLocationScope scope) = 0;

  virtual void stat_incr_fg_run_deadline_exceeded() = 0;

  // Histograms
  virtual void histogram_add_fg_runtime(size_t val) = 0;
  virtual void histogram_add_fg_run_event_loop_delay(size_t val) = 0;

  virtual ~FlowGroupDependencies() {}
};

class NwShapingFlowGroupDeps : public FlowGroupDependencies {
 public:
  NwShapingFlowGroupDeps(StatsHolder* stats) : stats_(stats) {}

  PerFlowGroupStats* statsGet(NodeLocationScope scope) {
    if (!stats_) {
      return nullptr;
    }
    return &(stats_->get().per_flow_group_stats[static_cast<int>(scope)]);
  }

  void stat_incr_fg_run_deadline_exceeded() override {
    STAT_INCR(stats_, flow_groups_run_deadline_exceeded);
  }

  void histogram_add_fg_runtime(size_t val) override {
    HISTOGRAM_ADD(stats_, flow_groups_run_time, val);
  }

  void histogram_add_fg_run_event_loop_delay(size_t val) override {
    HISTOGRAM_ADD(stats_, flow_groups_run_event_loop_delay, val);
  }

 private:
  StatsHolder* const stats_;
};

class ReadShapingFlowGroupDeps : public FlowGroupDependencies {
 public:
  ReadShapingFlowGroupDeps(StatsHolder* stats) : stats_(stats) {}

  PerFlowGroupStats* statsGet(NodeLocationScope scope) {
    if (!stats_) {
      return nullptr;
    }
    return &(stats_->get().per_flow_group_stats_rt[static_cast<int>(scope)]);
  }

  void stat_incr_fg_run_deadline_exceeded() override {
    STAT_INCR(stats_, flow_groups_run_deadline_exceeded_rt);
  }

  void histogram_add_fg_runtime(size_t val) override {
    HISTOGRAM_ADD(stats_, flow_groups_run_time_rt, val);
  }

  void histogram_add_fg_run_event_loop_delay(size_t val) {
    HISTOGRAM_ADD(stats_, flow_groups_run_event_loop_delay_rt, val);
  }

 private:
  StatsHolder* const stats_;
};

}} // namespace facebook::logdevice
