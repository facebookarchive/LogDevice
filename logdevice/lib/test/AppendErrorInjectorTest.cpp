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

  EXPECT_FALSE(injector.getCallback()(log, 0).hasValue());
}

TEST(AppendErrorInjectorTest, OneFailRatio) {
  logid_t log{1};
  AppendErrorInjector injector{Status::NOTFOUND, log, 1.0};

  EXPECT_EQ(Status::NOTFOUND, injector.getCallback()(log, 0));
}

TEST(AppendErrorInjectorTest, DifferentLogs) {
  logid_t log1{1}, log2{2}, log3{3};
  AppendErrorInjector injector{Status::SEQNOBUFS, {{log1, 1.0}, {log2, 1.0}}};

  EXPECT_EQ(Status::SEQNOBUFS, injector.getCallback()(log1, 0));
  EXPECT_EQ(Status::SEQNOBUFS, injector.getCallback()(log2, 0));
  EXPECT_FALSE(injector.getCallback()(log3, 0).hasValue());
}
