/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"

/**
 * @file Base class for integration tests.  Sets up logging, a timeout.
 */

namespace facebook { namespace logdevice {

class IntegrationTestBase : public ::testing::Test {
 protected:
  explicit IntegrationTestBase(
      std::chrono::milliseconds timeout = getDefaultTestTimeout())
      : timeout_(timeout), alarm_(timeout) {}

  void SetUp() override {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
    dbg::assertOnData = true;
  }

  std::chrono::milliseconds testTimeout() const {
    return timeout_;
  }

 private:
  std::chrono::milliseconds timeout_;
  Alarm alarm_;
};

}} // namespace facebook::logdevice
