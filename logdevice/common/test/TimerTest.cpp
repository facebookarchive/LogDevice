/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Timer.h"

#include <chrono>

#include <folly/Function.h>
#include <folly/MPMCQueue.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <gtest/gtest.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace std::chrono_literals;
using namespace std::chrono;

struct CallbackRequest : public Request {
  explicit CallbackRequest(folly::Function<void()>&& callback)
      : callback_(std::move(callback)) {}

  Request::Execution execute() override {
    callback_();
    return Request::Execution::COMPLETE;
  }

  folly::Function<void()> callback_;
};

TEST(Timer, Test) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto processor = make_test_processor(settings);

  auto promise = std::make_shared<folly::Promise<int>>();
  auto ready = promise->getSemiFuture();

  folly::Synchronized<std::vector<Timer>> vec;
  vec.wlock()->reserve(10);

  std::unique_ptr<Request> request = std::make_unique<CallbackRequest>([promise,
                                                                        &vec] {
    auto nfired = std::make_shared<std::atomic<int>>(0);
    auto tstart = steady_clock::now();
    auto assert_passed = [tstart](milliseconds ms) {
      ASSERT_GE(steady_clock::now() - tstart, 0.95 * ms);
    };

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(0ms);
    });
    vec.wlock()->back().activate(0ms);

    vec.wlock()->emplace_back([] { FAIL() << "timer not cancelled"; });
    vec.wlock()->back().activate(50ms);
    vec.wlock()->back().cancel();

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(20ms);
    });
    vec.wlock()->back().activate(20ms);

    vec.wlock()->emplace_back([] { FAIL() << "timer not cancelled"; });
    vec.wlock()->back().activate(50ms);
    vec.wlock()->back().cancel();

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(104ms);
    });
    vec.wlock()->back().activate(100ms);

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(101ms);
    });
    vec.wlock()->back().activate(10ms);
    vec.wlock()->back().activate(100ms);

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(102ms);
    });
    vec.wlock()->back().activate(microseconds(100000));

    vec.wlock()->emplace_back([nfired, assert_passed] {
      ++*nfired;
      assert_passed(103ms);
    });
    vec.wlock()->back().activate(
        duration_cast<microseconds>(duration<double>(0.1)));

    vec.wlock()->emplace_back([] { FAIL() << "timer not cancelled"; });
    vec.wlock()->back().activate(50ms);
    vec.wlock()->back().cancel();

    vec.wlock()->emplace_back(
        [nfired, promise] { promise->setValue(*nfired); });
    vec.wlock()->back().activate(1s);
  });

  ASSERT_EQ(processor->postRequest(request), 0);
  ASSERT_EQ(ready.valid(), true);
  std::move(ready).wait();
  ASSERT_EQ(ready.value(), 6);
}

TEST(Timer, RaceConditionInDestroy) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;

  folly::Synchronized<std::vector<Timer>> vec;
  vec.wlock()->reserve(1);

  {
    auto processor = make_test_processor(settings);

    std::unique_ptr<Request> request =
        std::make_unique<CallbackRequest>([&vec] {
          vec.wlock()->emplace_back([] { FAIL(); });
          vec.wlock()->back().activate(100ms);
        });
    ASSERT_EQ(processor->postRequest(request), 0);
  }
  folly::Baton<> baton;
  baton.try_wait_for(1s);
}
