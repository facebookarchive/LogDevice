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
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/test/TestUtil.h"

using namespace facebook::logdevice;

TEST(RequestUtilTest, fulfill) {
  constexpr int kGeneralWorkers = 7;
  constexpr int kBackgroundWorkers = 5;
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = kGeneralWorkers;
  ServerSettings server_settings = create_default_settings<ServerSettings>();
  server_settings.num_background_workers = kBackgroundWorkers;
  GossipSettings gossip_settings = create_default_settings<GossipSettings>();
  gossip_settings.enabled = true;

  auto processor_builder = TestServerProcessorBuilder{settings}
                               .setServerSettings(server_settings)
                               .setGossipSettings(gossip_settings);
  auto processor = std::move(processor_builder).build();
  SCOPE_EXIT {
    shutdown_test_server(processor);
  };

  {
    std::atomic<int> count{0};
    auto futures = fulfill_on_worker_pool<worker_id_t>(
        processor.get(),
        WorkerType::BACKGROUND,
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

    EXPECT_EQ(
        std::move(f).get(), kBackgroundWorkers * (kBackgroundWorkers - 1) / 2);
    EXPECT_EQ(kBackgroundWorkers, count.load());
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

    auto num_workers = processor->getAllWorkersCount();
    EXPECT_EQ(num_workers, std::move(f).get());
    EXPECT_GE(num_workers, kGeneralWorkers + kBackgroundWorkers);
  }
}
