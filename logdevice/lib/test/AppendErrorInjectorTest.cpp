/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/AppendErrorInjector.h"

#include <gtest/gtest.h>

#include "logdevice/lib/ClientImpl.h"

using namespace facebook::logdevice;

TEST(AppendErrorInjectorTest, ZeroFailRatio) {
  logid_t log{1};
  AppendErrorInjector injector{Status::SEQNOBUFS, log, 0.0};

  EXPECT_FALSE(injector.next(log));
}

TEST(AppendErrorInjectorTest, OneFailRatio) {
  logid_t log{1};
  AppendErrorInjector injector{Status::SEQNOBUFS, log, 1.0};

  EXPECT_TRUE(injector.next(log));
}

TEST(AppendErrorInjectorTest, ReturnsCorrectStatus) {
  EXPECT_EQ(
      Status::NOTFOUND,
      (AppendErrorInjector{Status::NOTFOUND, logid_t{1}, 0.0}.getErrorType()));
  EXPECT_EQ(
      Status::TIMEDOUT,
      (AppendErrorInjector{Status::TIMEDOUT, logid_t{1}, 0.0}.getErrorType()));
}

TEST(AppendErrorInjectorTest, DifferentLogs) {
  logid_t log1{1}, log2{2}, log3{3};
  AppendErrorInjector injector{Status::SEQNOBUFS, {{log1, 1.0}, {log2, 1.0}}};

  EXPECT_TRUE(injector.next(log1));
  EXPECT_TRUE(injector.next(log2));
  EXPECT_FALSE(injector.next(log3));
}
