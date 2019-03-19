/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/FlowGroupPolicy.h"

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

}} // namespace facebook::logdevice
