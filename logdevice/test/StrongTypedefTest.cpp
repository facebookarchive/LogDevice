/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/include/strong_typedef.h"
#include "logdevice/test/utils/IntegrationTestBase.h"

using namespace facebook::logdevice;

class StrongTypedefTest : public IntegrationTestBase {};

LOGDEVICE_STRONG_TYPEDEF(uint64_t, test_logdevice_type_t);

TEST_F(StrongTypedefTest, ComparisonOperatorsTest) {
  test_logdevice_type_t a(1);
  test_logdevice_type_t b(2);

  ASSERT_EQ(a, a);
  ASSERT_NE(a, b);

  ASSERT_LT(a, b);
  ASSERT_LE(a, a);
  ASSERT_GT(b, a);
  ASSERT_GE(b, b);
}
