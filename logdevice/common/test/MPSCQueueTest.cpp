/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MPSCQueue.h"

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/test/TestUtil.h"

TEST(MPSCQueueTest, Stress) {
  struct Item {
    int64_t val;
    folly::AtomicIntrusiveLinkedListHook<Item> hook;
  };

  const int64_t N = 300000;
  const int NPRODUCERS = 16;

  facebook::logdevice::MPSCQueue<Item, &Item::hook> queue;
  facebook::logdevice::Semaphore producer_start_sem;
  std::atomic<int64_t> counter(1), producers_done(0);
  auto one_producer = [&] {
    producer_start_sem.wait();
    while (1) {
      auto item = std::make_unique<Item>();
      item->val = counter++;
      if (item->val > N) {
        break;
      }
      queue.push(std::move(item));
    }
    ++producers_done;
  };
  std::vector<std::thread> producers;
  for (int i = 0; i < NPRODUCERS; ++i) {
    producers.emplace_back(one_producer);
  }

  std::vector<int64_t> seen;
  seen.reserve(N);
  for (size_t i = 0; i < producers.size(); ++i) {
    producer_start_sem.post();
  }

  facebook::logdevice::Alarm alarm{std::chrono::seconds(10)};
  while (1) {
    std::unique_ptr<Item> item = queue.pop();
    if (item) {
      seen.push_back(item->val);
    } else {
      if (producers_done.load() == NPRODUCERS) {
        break;
      }
      std::this_thread::yield();
    }
  }

  for (auto& th : producers) {
    th.join();
  }

  std::unordered_set<int64_t> seen_set(seen.begin(), seen.end());
  ASSERT_EQ(N, seen_set.size());
}
