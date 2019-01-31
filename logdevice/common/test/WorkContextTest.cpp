/**
 * Copyright (c) 2004-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/work_model/WorkContext.h"

#include <gtest/gtest.h>

#include "folly/executors/SerialExecutor.h"
using namespace ::testing;
using namespace facebook::logdevice;

class NOOPExecutor : public folly::Executor {
 public:
  NOOPExecutor() {}
  ~NOOPExecutor() override {}
  void add(folly::Func /* func */) override {}
  bool keepAliveAcquire() override {
    return true;
  }
  void keepAliveRelease() override {}
};

TEST(WorkContextTest, SimpleTest) {
  auto no_op_executor = std::make_unique<NOOPExecutor>();
  {
    auto serial_keep_alive = folly::SerialExecutor::create(
        folly::Executor::getKeepAliveToken(no_op_executor.get()));
    WorkContext ctx(std::move(serial_keep_alive));
    EXPECT_TRUE(ctx.anonymous());
  }
  {
    auto serial_keep_alive = folly::SerialExecutor::create(
        folly::Executor::getKeepAliveToken(no_op_executor.get()));
    WorkContext ctx(std::move(serial_keep_alive), 1);
    EXPECT_FALSE(ctx.anonymous());
    EXPECT_EQ(1, ctx.getId());
  }
}
