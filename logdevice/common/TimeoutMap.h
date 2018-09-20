/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "time.h"

#include <chrono>
#include <unordered_map>
#include <functional>

#include "event2/event.h"

namespace facebook { namespace logdevice {

class MicrosecondsHash {
 public:
  std::size_t operator()(std::chrono::microseconds const& ms) const {
    return ms.count();
  }
};

/**
 * Libevent normally stores all timer events in a heap with a O(logN)
 * insertion time. We expect to insert and delete timers at a high
 * rate, with few of them expiring. Because of the way LogDevice API
 * is designed most of the timers will have the same timeout value --
 * the one passed to Client::create() or Client::setTimeout(). We
 * expect the number of distinct timeout values passed to various
 * timed requests (mainly, AppendRequest) over the life of a Processor
 * to be small. Libevent has an optimization for such a workload that
 * reduces the cost of insertion to O(1). It works by creating a
 * separate FIFO queue for all timers with the same commonly used
 * timeout value. These timeout values must be registered in advance
 * by calling event_base_init_common_timeout(). It returns a queue
 * identifier masquarading as a struct timeval. TimeoutMap objects contain
 * a map from std::chrono::millisecond values to such queue identifiers
 * for a specific event_base. The objective is to reduce the amount of CPU
 * used for timer management in LogDevice.
 *
 * see http://www.wangafu.net/~nickm/libevent-2.1/doxygen/html/event_8h.html
 * for details.
 */

class TimeoutMap {
 public:
  TimeoutMap(struct event_base* base, int max_size);

  /**
   * Converts the timeout in microseconds to a struct timeval to be used as a
   * libevent timeout. The result may be a libevent "common timeout" or a
   * straightforward conversion into the supplied tv_buf.
   *
   * @return   if _timeout_ is in map_, return a pointer to the corresponding
   *           libevent timer queue identifier as a fake struct timeval. If the
   *           timeout value is not found and the map has fewer than max_size_
   *           entries, add an entry and return a new timer queue id. If the
   *           map is full, copy timeout into tv_buf converting it to struct
   *           timeval, and return tv_buf.
   */
  const struct timeval* get(std::chrono::microseconds timeout,
                            struct timeval* tv_buf);

 private:
  std::unordered_map<std::chrono::microseconds,
                     const struct timeval*,
                     MicrosecondsHash>
      map_;

  // event base for which we are generating timeout queue identifiers
  struct event_base* base_;

  // maximum number of entries we can have in map_
  const int max_size_;
};

}} // namespace facebook::logdevice
