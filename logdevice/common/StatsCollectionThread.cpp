/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "StatsCollectionThread.h"

#include <folly/Optional.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

StatsCollectionThread::StatsCollectionThread(
    const StatsHolder* source,
    std::chrono::seconds interval,
    std::unique_ptr<StatsPublisher> publisher)
    : source_stats_(source),
      interval_(interval),
      publisher_(std::move(publisher)),
      thread_(std::bind(&StatsCollectionThread::mainLoop, this)) {
  ld_debug("Stats Collection Thread Started...");
}

StatsCollectionThread::~StatsCollectionThread() {
  shutDown();
  thread_.join();
}

namespace {
struct StatsSnapshot {
  Stats stats;
  std::chrono::steady_clock::time_point when;
};
} // namespace

void StatsCollectionThread::mainLoop() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:stats");

  folly::Optional<StatsSnapshot> previous;

  while (true) {
    using namespace std::chrono;
    const auto now = steady_clock::now();
    const auto next_tick = now + interval_;
    StatsSnapshot current = {source_stats_->aggregate(), now};
    ld_debug("Publishing Stats...");
    if (previous.hasValue()) {
      publisher_->publish(
          current.stats,
          previous.value().stats,
          duration_cast<milliseconds>(now - previous.value().when));
    }
    previous.assign(std::move(current));

    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_) {
        break;
      }
      if (next_tick > steady_clock::now()) {
        cv_.wait_until(lock, next_tick, [this]() { return stop_; });
      }
      // NOTE: even if we got woken up by shutDown(), we still aggregate and
      // push once more so we don't lose data from the partial interval
    }
  }
}

}} // namespace facebook::logdevice
