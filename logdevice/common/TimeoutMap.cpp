/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TimeoutMap.h"

#include <event2/event.h>

#include "logdevice/common/checks.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

TimeoutMap::TimeoutMap(struct event_base* base, int max_size)
    : base_(base), max_size_(max_size) {
  ld_check(base);
  ld_check(max_size >= 0);
}

bool TimeoutMap::add(std::chrono::microseconds timeout,
                     const struct timeval* tv_buf) {
  if (map_.size() >= static_cast<unsigned>(max_size_))
    return false;

  map_.insert(std::make_pair(timeout, tv_buf));
  return true;
}

const struct timeval* TimeoutMap::get(std::chrono::microseconds timeout,
                                      struct timeval* tv_buf) {
  ld_check(tv_buf);

  const auto pos = map_.find(timeout);

  if (pos != map_.end()) {
    ld_check(pos->second);
    return pos->second;
  }

  tv_buf->tv_sec = timeout.count() / 1000000;
  tv_buf->tv_usec = timeout.count() % 1000000;

  if (map_.size() < static_cast<unsigned>(max_size_)) {
    const struct timeval* timer_queue_id =
        LD_EV(event_base_init_common_timeout)(base_, tv_buf);
    ld_check(timer_queue_id);

    auto res = map_.insert(std::make_pair(timeout, timer_queue_id));
    ld_check(res.second);

    return timer_queue_id;
  }

  return tv_buf;
}

}} // namespace facebook::logdevice
