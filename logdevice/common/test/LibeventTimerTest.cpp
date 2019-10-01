/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LibeventTimer.h"

#include <chrono>

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "logdevice/common/libevent/LibEventCompatibility.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"

using namespace facebook::logdevice;

constexpr std::chrono::milliseconds
/* implicit */
operator"" _ms(unsigned long long val) {
  return std::chrono::milliseconds(val);
}

class LibeventTimerTest
    : public testing::TestWithParam<facebook::logdevice::EvBase::EvBaseType> {};

TEST_P(LibeventTimerTest, Test) {
  using namespace std::chrono;
  EvBase base;
  base.selectEvBase(GetParam());
  base.setAsRunningBase();
  auto rv = base.init();
  ASSERT_EQ(rv, EvBase::Status::OK);

  auto tstart = steady_clock::now();
  auto assert_passed = [tstart](milliseconds ms) {
    ASSERT_GE(steady_clock::now() - tstart, 0.95 * ms);
  };

  int nfired = 0;

  LibeventTimer t20(&base, [&] {
    ++nfired;
    assert_passed(15_ms);
  });
  t20.activate(20_ms);

  LibeventTimer t50_cancel(&base, [] { FAIL() << "timer not cancelled"; });
  t50_cancel.activate(50_ms);
  t50_cancel.cancel();

  LibeventTimer t100(&base, [&] {
    ++nfired;
    assert_passed(95_ms);
  });
  t100.activate(100_ms);

  LibeventTimer t100_change(&base, [&] {
    ++nfired;
    assert_passed(95_ms);
  });
  t100_change.activate(10_ms);
  t100_change.activate(100_ms);

  LibeventTimer t100_us(&base, [&] {
    ++nfired;
    assert_passed(95_ms);
  });
  t100_us.activate(microseconds(100000));

  LibeventTimer t100_double(&base, [&] {
    ++nfired;
    assert_passed(95_ms);
  });
  t100_double.activate(duration_cast<microseconds>(duration<double>(0.1)));

  base.loop();
  ASSERT_EQ(5, nfired);
}
