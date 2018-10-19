/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice;
using ErrorCodeInfoMap = EnumMap<E, ErrorCodeInfo, E::UNKNOWN>;

TEST(ErrorStringTest, ValidString) {
#define ERROR_CODE(id, val, str)                  \
  do {                                            \
    ASSERT_NE(error_name(E::id), nullptr);        \
    ASSERT_NE(error_description(E::id), nullptr); \
  } while (0);
#include "logdevice/include/errors.inc"

  ASSERT_EQ(ErrorCodeInfoMap::invalidValue(), errorStrings()[E::UNKNOWN]);
  ASSERT_EQ(ErrorCodeInfoMap::invalidValue(), errorStrings()[E::MAX]);
  ASSERT_EQ(ErrorCodeInfoMap::invalidValue(),
            errorStrings()[static_cast<int>(E::MAX) + 1]);
  ASSERT_EQ(ErrorCodeInfoMap::invalidValue(), errorStrings()[-1]);
  ASSERT_NE(errorStrings()[E::UNKNOWN].name, nullptr);
  ASSERT_NE(errorStrings()[E::UNKNOWN].description, nullptr);
  ASSERT_NE(error_name(E::UNKNOWN), nullptr);
  ASSERT_NE(error_description(E::UNKNOWN), nullptr);
}
