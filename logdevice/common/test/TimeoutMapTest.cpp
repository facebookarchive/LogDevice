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
  int i;
  struct event_base* base = LD_EV(event_base_new)();
  SCOPE_EXIT {
    LD_EV(event_base_free)(base);
  };
  TimeoutMap tm(base, TIMEOUT_MAP_SIZE);
  struct timeval tv_buf;

  for (i = 0; i < TIMEOUT_MAP_SIZE; i++) {
    const struct timeval* tqid = tm.get(std::chrono::milliseconds(i), &tv_buf);
    ASSERT_FALSE(tqid->tv_sec == 0 && tqid->tv_usec == i * 1000);
    ASSERT_NE(tqid, &tv_buf);
  }

  for (i = 0; i < TIMEOUT_MAP_SIZE; i++) {
    const struct timeval* tqid = tm.get(std::chrono::milliseconds(i), &tv_buf);
    ASSERT_FALSE(tqid->tv_sec == 0 && tqid->tv_usec == i * 1000);
    ASSERT_NE(tqid, &tv_buf);
  }

  const struct timeval* tmo = tm.get(std::chrono::milliseconds(i), &tv_buf);
  ASSERT_TRUE(tmo->tv_sec == 0 && tmo->tv_usec == i * 1000);
  ASSERT_EQ(tmo, &tv_buf);
}
