/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/libevent/EvBase.h"

#include <gtest/gtest.h>

namespace {

class EvBaseTest : public ::testing::Test {};

} // namespace

namespace facebook { namespace logdevice {

TEST(EvBaseTest, ConstructFree) {
  using S = EvBase::Status;
  EvBase base;
  EXPECT_EQ(S::OK, base.free());
  EXPECT_EQ(nullptr, base.getRawBaseDEPRECATED());
  EXPECT_EQ(S::NOT_INITIALIZED, base.loopOnce());
  EXPECT_EQ(S::OK, base.init());
  EXPECT_NE(nullptr, base.getRawBaseDEPRECATED());
  EXPECT_EQ(S::ALREADY_INITIALIZED, base.init());
  EXPECT_EQ(S::OK, base.free());
  EXPECT_EQ(nullptr, base.getRawBaseDEPRECATED());
  EXPECT_EQ(S::OK, base.init());
  EXPECT_NE(nullptr, base.getRawBaseDEPRECATED());
}

TEST(EvBaseTest, InitPriorities) {
  using S = EvBase::Status;
  EvBase base;
  EXPECT_EQ(S::INVALID_PRIORITY, base.init(-1));
  EXPECT_EQ(
      S::INVALID_PRIORITY,
      base.init(static_cast<int>(EvBase::Priorities::MAX_PRIORITIES) + 1));
  EXPECT_EQ(S::OK, base.init(1));
  EXPECT_EQ(S::ALREADY_INITIALIZED, base.init());
}

TEST(EvBaseTest, LoopOnce) {
  using S = EvBase::Status;
  EvBase base;
  EXPECT_EQ(S::NOT_INITIALIZED, base.loopOnce());
  EXPECT_EQ(S::OK, base.init());
  EXPECT_EQ(S::OK, base.loopOnce());
}

}} // namespace facebook::logdevice
