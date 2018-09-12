/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TimeoutMap.h"

#include "logdevice/common/libevent/compat.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

TimeoutMap::TimeoutMap(struct event_base* base, int max_size)
    : base_(base), max_size_(max_size) {
  ld_check(base);
  ld_check(max_size >= 0);
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

  if (map_.size() < (unsigned)max_size_) {
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
