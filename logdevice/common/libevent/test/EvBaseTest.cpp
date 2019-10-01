/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/libevent/LibEventCompatibility.h"

namespace {

class EvBaseTest
    : public testing::TestWithParam<facebook::logdevice::EvBase::EvBaseType> {};

} // namespace

namespace facebook { namespace logdevice {

TEST_P(EvBaseTest, InitPriorities) {
  using S = EvBase::Status;
  EvBase base;
  base.selectEvBase(GetParam());
  EXPECT_EQ(S::INVALID_PRIORITY, base.init(-1));
  EXPECT_EQ(
      S::INVALID_PRIORITY,
      base.init(static_cast<int>(EvBase::Priorities::MAX_PRIORITIES) + 1));
  EXPECT_EQ(S::OK, base.init(1));
}

TEST_P(EvBaseTest, LoopOnce) {
  using S = EvBase::Status;
  EvBase base;
  base.selectEvBase(GetParam());
  EXPECT_EQ(S::OK, base.init());
  EXPECT_EQ(S::OK, base.loopOnce());
}

INSTANTIATE_TEST_CASE_P(TestAllBase,
                        EvBaseTest,
                        ::testing::Values(EvBase::EvBaseType::LEGACY_EVENTBASE,
                                          EvBase::EvBaseType::FOLLY_EVENTBASE));
}} // namespace facebook::logdevice
