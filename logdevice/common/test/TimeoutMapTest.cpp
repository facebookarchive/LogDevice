/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TimeoutMap.h"

#include <event2/event.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "logdevice/common/libevent/compat.h"

using namespace facebook::logdevice;

#define TIMEOUT_MAP_SIZE 10

TEST(TimeoutMap, Correctness) {
  event_base* base = LD_EV(event_base_new)();
  SCOPE_EXIT {
    LD_EV(event_base_free)(base);
  };
  TimeoutMap tm(base, TIMEOUT_MAP_SIZE);
  std::vector<std::pair<int, const timeval*>> timers;
  for (int i = 0; i < TIMEOUT_MAP_SIZE; ++i) {
    const timeval* tqid = tm.get(std::chrono::milliseconds(i));
    ASSERT_NE(nullptr, tqid);
    timers.push_back(std::pair<int, const timeval*>(i, tqid));
  }

  for (const auto& timer : timers) {
    const timeval* tqid = tm.get(std::chrono::milliseconds(timer.first));
    ASSERT_EQ(timer.second, tqid);
  }

  const timeval* tmo = tm.get(std::chrono::milliseconds(TIMEOUT_MAP_SIZE));
  ASSERT_EQ(nullptr, tmo);
}
