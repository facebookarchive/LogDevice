/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

TEST(RequestUtilTest, fulfill) {
  constexpr int kWorkers = 5;
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = kWorkers;
  auto processor = make_test_processor(settings);

  {
    std::atomic<int> count{0};
    auto futures = fulfill_on_worker_pool<worker_id_t>(
        processor.get(),
        WorkerType::GENERAL,
        [](folly::Promise<worker_id_t> p) {
          Worker* w = Worker::onThisThread();
          ASSERT_TRUE(w);
          p.setValue(w->idx_);
        });

    folly::Future<int> f = folly::unorderedReduce(
        futures.begin(), futures.end(), 0, [&count](int a, worker_id_t&& b) {
          count.fetch_add(1);
          return a + b.val();
        });

    EXPECT_EQ(std::move(f).get(), kWorkers * (kWorkers - 1) / 2);
    EXPECT_EQ(kWorkers, count.load());
  }

  {
    auto futures = fulfill_on_all_workers<folly::Unit>(
        processor.get(), [](folly::Promise<folly::Unit> p) {
          Worker* w = Worker::onThisThread();
          ASSERT_TRUE(w);
          p.setValue();
        });

    auto f = folly::unorderedReduce(
        futures.begin(), futures.end(), 0, [](int a, folly::Unit&&) {
          return a + 1;
        });

    EXPECT_EQ(std::move(f).get(), processor->getAllWorkersCount());
  }
}
