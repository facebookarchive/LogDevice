/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TimeoutMap.h"

#include <event2/event.h>
#include <gtest/gtest.h>

#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"

using namespace facebook::logdevice;
using namespace testing;

#define TIMEOUT_MAP_SIZE 10

TEST(TimeoutMap, Correctness) {
  auto base_fake = LD_EV(event_base_new)();
  EvBaseMock base(true /* legacy */);
  base.init();
  base.setAsRunningBase();

  EXPECT_CALL(base, getRawBaseDEPRECATED())
      .Times(Exactly(TIMEOUT_MAP_SIZE))
      .WillRepeatedly(Return(base_fake));

  TimeoutMap tm(TIMEOUT_MAP_SIZE);
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

  LD_EV(event_base_free)(base_fake);
}
