/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TimeoutMap.h"

#include "logdevice/common/libevent/LibEventCompatibility.h"

namespace facebook { namespace logdevice {

TimeoutMap::TimeoutMap(uint32_t max_size) : max_size_(max_size) {}

const timeval* TimeoutMap::add(std::chrono::microseconds timeout) {
  if (map_.size() >= max_size_) {
    return nullptr;
  }
  const timeval* timer_queue_id = EvTimer::getCommonTimeout(timeout);
  if (timer_queue_id) {
    map_.insert(std::make_pair(timeout, timer_queue_id));
  }
  return timer_queue_id;
}

const timeval* TimeoutMap::get(std::chrono::microseconds timeout) {
  const auto pos = map_.find(timeout);

  if (pos != map_.end()) {
    return pos->second;
  }

  return add(timeout);
}

}} // namespace facebook::logdevice
