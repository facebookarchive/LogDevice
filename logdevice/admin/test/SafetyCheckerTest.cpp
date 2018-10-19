/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/SafetyChecker.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;

TEST(SafetyCheckerTest, Parse) {
  SafetyMargin safety_margin1;
  ASSERT_EQ(0, parseSafetyMargin("rack:3", safety_margin1));
  ASSERT_EQ(3, safety_margin1[NodeLocationScope::RACK]);

  SafetyMargin safety_margin2;
  ASSERT_EQ(0, parseSafetyMargin("rack:2,node:5", safety_margin2));
  ASSERT_EQ(2, safety_margin2[NodeLocationScope::RACK]);
  ASSERT_EQ(5, safety_margin2[NodeLocationScope::NODE]);
}
