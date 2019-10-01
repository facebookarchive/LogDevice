/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/ScopeGuard.h>
#include <folly/Singleton.h>

#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/WheelTimer.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/LibEventCompatibility.h"

using namespace facebook::logdevice;
using namespace std::chrono_literals;
using namespace std::chrono;

BENCHMARK(TimerBenchmarkSequential, n) {
  std::unique_ptr<WheelTimer> wheel;
  BENCHMARK_SUSPEND {
    wheel = std::make_unique<WheelTimer>();
  }
  for (int i = 0; i < n; ++i) {
    std::atomic<int> counter{0};
    wheel->createTimer(
        [&counter] { counter.fetch_add(1, std::memory_order_relaxed); }, 0ms);
    while (counter.load(std::memory_order_relaxed) != 1) {
    }
  }
}

BENCHMARK(TimerBenchmarkParallel, n) {
  std::unique_ptr<WheelTimer> wheel;
  BENCHMARK_SUSPEND {
    wheel = std::make_unique<WheelTimer>();
  }
  std::atomic<int> counter{0};
  for (int i = 0; i < n; ++i) {
    wheel->createTimer(
        [&counter] { counter.fetch_add(1, std::memory_order_relaxed); }, 0ms);
  }
  while (counter.load(std::memory_order_relaxed) != n) {
  }
}

BENCHMARK_RELATIVE(LibeventTimerParallel, n) {
  dbg::currentLevel = dbg::Level::NONE;
  std::unique_ptr<EvBase> base;
  std::vector<std::unique_ptr<LibeventTimer>> timers;
  BENCHMARK_SUSPEND {
    base = std::make_unique<EvBase>();
    auto rv = base->init();
    assert(rv == EvBase::Status::OK);
    timers.reserve(n);
  }

  int nfired = 0;
  for (int i = 0; i < n; ++i) {
    timers.emplace_back(
        std::make_unique<LibeventTimer>(base.get(), [&] { ++nfired; }));
    timers.back()->activate(0ms);
  }
  base->loop();
  while (nfired != n) {
  }
}

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  dbg::currentLevel = dbg::Level::ERROR;
  folly::SingletonVault::singleton()->registrationComplete();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
#endif
