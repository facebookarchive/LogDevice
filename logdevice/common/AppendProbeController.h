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
#include <unordered_map>

#include <folly/SharedMutex.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * When a sequencer has a hiccup and responds to some append with some variant
 * of "Failed: I'm overloaded", this class can make note of that and instruct
 * further appends to the same sequencer to send probes before actual
 * payloads.  That way, especially with large payloads such as those generated
 * by BufferedWriter in high-throughput scenarios, we avoid overloading the
 * cluster with payload bytes that cannot be processed.
 *
 * The input to the class are append failures and successes.  The output is a
 * binary recommendation for each append, to send a probe before the actual
 * append or not.
 *
 * The current logic is to recommend a probe if there was a recent failure and
 * there hasn't been a "recovery interval" since then during which all appends
 * succeeded.  The purpose of the recovery interval is to avoid flapping when
 * the cluster is accepting some but not all appends.
 *
 * This class is thread-safe.
 */

class AppendProbeController {
 public:
  explicit AppendProbeController(std::chrono::milliseconds recovery_interval)
      : recovery_interval_(recovery_interval) {}

  /**
   * Called by AppendRequest to decide if the append should be preceded by a
   * probe.
   */
  bool shouldProbe(NodeID, logid_t);

  /**
   * Called by AppendRequest when it receives a reply from a sequencer (also
   * when an append times out which is technically not a reply).
   *
   * NOTE: the Status here is what we get from the sequencer over the wire,
   * not necessarily what we report to the client.  The wire status contains
   * more information and may allow us to better determine whether to probe.
   */
  void onAppendReply(NodeID node_id, logid_t log_id, Status wire_status) {
    if (wire_status == E::OK) {
      onSuccess(node_id, log_id);
    } else {
      onFailure(node_id, log_id, wire_status);
    }
  }

  virtual ~AppendProbeController() {}

 protected:
  using TimePoint = std::chrono::steady_clock::time_point::duration;
  virtual TimePoint now() const {
    return std::chrono::steady_clock::now().time_since_epoch();
  }

 private:
  const std::chrono::milliseconds recovery_interval_;

  struct State {
    enum class Enum {
      // No recent failures.  Typically the `State' instance will only exist
      // for an extremely short time as we delete instances in this state.
      HEALTHY,
      // Most recent append failed.  Probes recommended.
      LAST_APPEND_FAILED,
      // There were failures but also successes since the last failure.
      // Probes recommended for the time being but node will be considered
      // healthy at `healthy_at' if no more failures are observed.
      RECOVERING,
    };
    explicit State(Enum initial_state) : e(initial_state) {}
    std::atomic<Enum> e;
    std::atomic<TimePoint> healthy_at;
  };
  using NodeStateMap = std::unordered_map<NodeID, State, NodeID::Hash>;
  NodeStateMap node_state_map_;
  // Mutex acquired for writing for map insert/erase, which only happens
  // when transitioning to/from HEALTHY, the frequency of which is limited to
  // once per `recovery_interval_' per node.
  folly::SharedMutex node_state_map_mutex_;

  void onFailure(NodeID, logid_t, Status);
  void onSuccess(NodeID, logid_t);
  void garbageCollectIfHealthy(NodeID);
};

}} // namespace facebook::logdevice
