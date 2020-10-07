/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/test/ThriftApiIntegrationTestBase.h"

namespace facebook { namespace logdevice { namespace test { namespace {

class ThriftApiIntegrationTest : public ThriftApiIntegrationTestBase {};

TEST_F(ThriftApiIntegrationTest, TestSingleRpcCall) {
  checkSingleRpcCall();
}

}}}} // namespace facebook::logdevice::test::
