/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

namespace facebook { namespace logdevice {

struct WorkerTimeSeriesStats {
  int worker_id{-1};
  int curr_num{0};
  int curr_sum{0};
  double curr_avg{0};
};

/**
 * Record health stats of a node.
 */
struct NodeHealthStats {
  int observed_time_period_ms{-1};
  bool health_monitor_delayed{false};
  bool watchdog_delayed{false};
  int curr_total_stalled_workers{0};
  // Stats relating to stalled workers:
  double percent_workers_w_long_exe_req{0};
  std::vector<WorkerTimeSeriesStats> stalled_worker_stats;
  // Stats relating to overloaded workers
  double percent_workers_w_long_queued_req{0};
  std::vector<WorkerTimeSeriesStats> overloaded_worker_stats;
  // Stat relating to fd overload:
  int curr_num_conn_req_rejected{0};
  // Stat relating to state timer:
  int curr_state_timer_value_ms{0};
};
}} // namespace facebook::logdevice
