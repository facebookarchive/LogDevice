/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/UpdateableSharedPtr.h"

#include <atomic>
#include <random>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/checks.h"

using namespace facebook::logdevice;

struct TestObject {
  int value;
  std::atomic<int>& counter;

  TestObject(int value, std::atomic<int>& counter)
      : value(value), counter(counter) {
    ++counter;
  }

  ~TestObject() {
    ld_check(counter.load() > 0);
    --counter;
  }
};

TEST(UpdateableSharedPtrTest, StressTest) {
  const int ptr_count = 2;
  const int thread_count = 5;
  const std::chrono::milliseconds duration(100);
  const std::chrono::milliseconds upd_delay(1);
  const std::chrono::milliseconds respawn_delay(1);

  struct Instance {
    std::atomic<int> value[2]{{0}, {0}};
    std::atomic<int> prev_value[2]{{0}, {0}};
    UpdateableSharedPtr<TestObject> ptr1;
    FastUpdateableSharedPtr<TestObject> ptr2;
  };

  struct Thread {
    std::thread t;
    std::atomic<bool> shutdown{false};
  };

  std::atomic<int> counter(0);
  std::vector<Instance> instances(ptr_count);
  std::vector<Thread> threads(thread_count);
  std::atomic<int> seed(0);

  // Threads that call get() and checking value.
  auto thread_func = [&](int t) {
    std::mt19937 rnd(++seed);
    while (!threads[t].shutdown.load()) {
      Instance& instance = instances[rnd() % instances.size()];
      int i = rnd() % 2;
      int val1 = instance.prev_value[i].load();
      int val;
      if (i == 0) {
        auto p = instance.ptr1.get();
        val = p ? p->value : 0;
      } else {
        auto p = instance.ptr2.get();
        val = p ? p->value : 0;
      }
      int val2 = instance.value[i].load();
      EXPECT_LE(val1, val);
      EXPECT_LE(val, val2);
    }
  };

  for (size_t t = 0; t < threads.size(); ++t) {
    threads[t].t = std::thread(thread_func, t);
  }

  std::atomic<bool> shutdown(false);

  // Thread that calls update() occasionally.
  std::thread update_thread([&]() {
    std::mt19937 rnd(++seed);
    while (!shutdown.load()) {
      Instance& instance = instances[rnd() % instances.size()];
      int i = rnd() % 2;
      int val = ++instance.value[i];
      if (i == 0) {
        instance.ptr1.update(std::make_shared<TestObject>(val, counter));
      } else {
        instance.ptr2.update(std::make_shared<TestObject>(val, counter));
      }
      ++instance.prev_value[i];
      /* sleep override */
      std::this_thread::sleep_for(upd_delay);
    }
  });

  // Thread that joins and spawns get() threads occasionally.
  std::thread respawn_thread([&]() {
    std::mt19937 rnd(++seed);
    while (!shutdown.load()) {
      int t = rnd() % threads.size();
      threads[t].shutdown.store(true);
      threads[t].t.join();
      threads[t].shutdown.store(false);
      threads[t].t = std::thread(thread_func, t);

      /* sleep override */
      std::this_thread::sleep_for(respawn_delay);
    }
  });

  // Let all of this run for some time.
  /* sleep override */
  std::this_thread::sleep_for(duration);

  // Shut all of this down.
  shutdown.store(true);

  update_thread.join();
  respawn_thread.join();
  for (auto& t : threads) {
    t.shutdown.store(true);
    t.t.join();
  }

  for (auto& instance : instances) {
    instance.ptr1.update(nullptr);
    instance.ptr2.update(nullptr);
    EXPECT_EQ(instance.value[0].load(), instance.prev_value[0].load());
    EXPECT_EQ(instance.value[1].load(), instance.prev_value[1].load());
  }

  EXPECT_EQ(0, counter.load());
}
