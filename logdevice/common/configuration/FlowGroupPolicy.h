/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <chrono>

#include "logdevice/common/FlowMeter.h"
#include "logdevice/common/Priority.h"

namespace facebook { namespace logdevice {

/**
 * @file  A FlowGroupPolicy dictates the behavior of a FlowGroup. See the
 *        comment block in FlowGroup.h for more details of FlowGroups and
 *        the classes related to them.
 */

class FlowGroupPolicy {
 public:
  struct Entry {
    // NOTE: FlowGroupMeters can go negative (into debt). This is required
    //       in order to support messages that are larger than the "leaky
    //       bucket size". To eliminate the chance of subtle bugs introduced
    //       through mixed signed/unsigned operations, even values like the
    //       budget values below that can never be negative are expressed
    //       using signed integers.

    // Maximum bandwidth credit that can be accumulated (i.e. maximum burst)
    // in bytes.
    int64_t capacity = 0;
    // Guaranteed bandwidth in bytes per-scope/quantum
    // (global/1s for config/admin updates, per-worker/update-period
    //  when provided in a FlowGroupsUpdate).
    int64_t guaranteed_bw = 0;

    // Bandwidth limit which is enforced even if excess/unused bandwidth
    // from other priority classes is available.
    int64_t max_bw = INT64_MAX;
  };

  bool configured() const {
    return configured_;
  }
  bool enabled() const {
    return enabled_;
  }

  void setConfigured(bool configured) {
    configured_ = configured;
  }

  void setEnabled(bool enable) {
    enabled_ = enable;
  }

  void set(Priority p,
           int64_t capacity,
           int64_t guaranteed_bw,
           int64_t max_bw = INT64_MAX) {
    int idx = asInt(p);
    auto& bucket_policy = entries[idx];
    bucket_policy.capacity = capacity;
    // Input validation occurs for both admin commands and during
    // config processing. Clamping shouldn't be required.
    ld_check(max_bw >= guaranteed_bw);
    bucket_policy.guaranteed_bw = std::min(guaranteed_bw, max_bw);
    bucket_policy.max_bw = max_bw;
    configured_ = true;
  }

  /** Scale a config/admin defined policy to be per-worker/update-period. */
  FlowGroupPolicy normalize(size_t num_workers,
                            std::chrono::microseconds interval) const;

  /**
   * Convenience function for accessing the bandwidth policy for
   * shared priority queue.
   */
  Entry& priorityQEntry() {
    return entries.back();
  }

  // The policies for all message priorities for a flow.
  //
  // The entry at Priority::NUM_PRIORITIES contains the policy
  // for bandwidth shared between priorities that is allocated in
  // priority order after each run of the EventLoop.
  std::array<Entry, asInt(Priority::NUM_PRIORITIES) + 1> entries;

 private:
  bool configured_ = false;
  bool enabled_ = false;
};

}} // namespace facebook::logdevice
