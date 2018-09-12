/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "FlowGroupPolicy.h"

#include <folly/dynamic.h>
#include "logdevice/common/PriorityMap.h"

namespace facebook { namespace logdevice {

FlowGroupPolicy
FlowGroupPolicy::normalize(size_t num_workers,
                           std::chrono::microseconds interval) const {
  FlowGroupPolicy result(*this);
  std::chrono::seconds sec(1);
  size_t portions =
      (std::chrono::duration_cast<decltype(interval)>(sec).count() *
       num_workers) /
      interval.count();
  for (auto& e : result.entries) {
    e.capacity = e.capacity / num_workers;
    e.guaranteed_bw = e.guaranteed_bw / portions;
    // INT64_MAX means no-cap.
    if (e.max_bw != INT64_MAX) {
      e.max_bw = e.max_bw / portions;
    }
  }
  return result;
}

folly::dynamic FlowGroupPolicy::toFollyDynamic(NodeLocationScope scope) const {
  folly::dynamic result =
      folly::dynamic::object("name", NodeLocation::scopeNames()[scope])(
          "shaping_enabled", trafficShapingEnabled());

  folly::dynamic meter_list = folly::dynamic::array;
  Priority p = Priority::MAX;
  for (const auto& entry : entries) {
    if (entry.guaranteed_bw != 0 || entry.capacity != 0) {
      meter_list.push_back(entry.toFollyDynamic(p));
    }
    p = priorityBelow(p);
  }
  if (!meter_list.empty()) {
    result["meters"] = meter_list;
  }

  return result;
}

folly::dynamic FlowGroupPolicy::Entry::toFollyDynamic(Priority priority) const {
  folly::dynamic result = folly::dynamic::object(
      "name",
      priority == Priority::INVALID ? "PRIORITY_QUEUE"
                                    : PriorityMap::toName()[priority])(
      "guaranteed_bytes_per_second", guaranteed_bw)(
      "max_burst_bytes", capacity);

  if (max_bw != INT64_MAX) {
    result.insert("max_bytes_per_second", max_bw);
  }

  return result;
}

}} // namespace facebook::logdevice
