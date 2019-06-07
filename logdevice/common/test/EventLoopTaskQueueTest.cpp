/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EventLoopTaskQueue.h"

#include <memory>

#include <folly/Executor.h>
#include <gtest/gtest.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Semaphore.h"

using namespace facebook::logdevice;

TEST(EventLoopTaskQueue, TranslatePriority) {
  ASSERT_EQ(EventLoopTaskQueue::translatePriority(folly::Executor::HI_PRI), 0);
  ASSERT_EQ(EventLoopTaskQueue::translatePriority(folly::Executor::MID_PRI), 1);
  ASSERT_EQ(EventLoopTaskQueue::translatePriority(folly::Executor::LO_PRI), 2);

  ASSERT_NE(EventLoopTaskQueue::translatePriority(folly::Executor::LO_PRI),
            EventLoopTaskQueue::translatePriority(folly::Executor::HI_PRI));

  ASSERT_EQ(EventLoopTaskQueue::translatePriority(105),
            EventLoopTaskQueue::translatePriority(folly::Executor::LO_PRI));
}

TEST(EventLoopTaskQueue, PostPrioritizedWork) {
  auto el = std::make_unique<EventLoop>();
  size_t num_hi_pri_task = 500;
  size_t num_mid_pri_task = 2;
  size_t num_lo_pri_task = 1;

  size_t num_lo_pri_executed = 0;
  size_t num_mid_pri_executed = 0;
  size_t num_hi_pri_executed = 0;
  Semaphore start_loop, primed;
  el->add([&el, &start_loop, &primed]() {
    el->getTaskQueue().setDequeuesPerIteration({7, 2, 1});
    primed.post();
    start_loop.wait();
  });
  primed.wait();
  for (auto i = 0; i < 7; ++i) {
    el->addWithPriority([&num_hi_pri_executed] { ++num_hi_pri_executed; },
                        folly::Executor::HI_PRI);
  }

  Semaphore block, ready;
  el->addWithPriority(
      [&block, &ready, &num_hi_pri_executed] {
        ready.post();
        block.wait();
        ++num_hi_pri_executed;
      },
      folly::Executor::HI_PRI);

  for (auto i = 0; i < num_hi_pri_task; ++i) {
    el->addWithPriority([&num_hi_pri_executed] { ++num_hi_pri_executed; },
                        folly::Executor::HI_PRI);
  }

  for (auto i = 0; i < num_mid_pri_task; ++i) {
    el->addWithPriority([&num_mid_pri_executed] { ++num_mid_pri_executed; },
                        folly::Executor::MID_PRI);
  }

  for (auto i = 0; i < num_lo_pri_task; ++i) {
    el->addWithPriority([&num_lo_pri_executed] { ++num_lo_pri_executed; },
                        folly::Executor::LO_PRI);
  }
  start_loop.post();
  ready.wait();

  EXPECT_EQ(7, num_hi_pri_executed);
  EXPECT_EQ(num_mid_pri_task, num_mid_pri_executed);
  EXPECT_EQ(num_lo_pri_task, num_lo_pri_executed);

  block.post();

  Semaphore gate;
  el->addWithPriority(
      [&gate, &num_hi_pri_executed] {
        ++num_hi_pri_executed;
        gate.post();
      },
      folly::Executor::HI_PRI);
  gate.wait();
  EXPECT_EQ(7 + 1 + num_hi_pri_task + 1, num_hi_pri_executed);
  EXPECT_EQ(num_mid_pri_task, num_mid_pri_executed);
  EXPECT_EQ(num_lo_pri_task, num_lo_pri_executed);
}
