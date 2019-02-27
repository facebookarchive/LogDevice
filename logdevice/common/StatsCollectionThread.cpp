/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/StatsCollectionThread.h"

#include <folly/Optional.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/StatsPublisherFactory.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

StatsCollectionThread::StatsCollectionThread(
    const StatsHolder* source,
    std::chrono::seconds interval,
    std::unique_ptr<StatsPublisher> publisher)
    : source_stats_sets_({source}),
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
struct StatsSnapshots {
  std::vector<Stats> stats;
  std::chrono::steady_clock::time_point when;
};
} // namespace

void StatsCollectionThread::addStatsSource(const StatsHolder* source) {
  std::unique_lock<std::mutex> lock(mutex_);
  source_stats_sets_.push_back(source);
}

void StatsCollectionThread::mainLoop() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:stats");

  using namespace std::chrono;

  StatsSnapshots previous_snapshots{{}, steady_clock::now()};

  while (true) {
    const auto now = steady_clock::now();
    const auto next_tick = now + interval_;

    StatsSnapshots current_snapshots{{}, now};

    {
      std::unique_lock<std::mutex> lock(mutex_);
      for (const StatsHolder* source_stats_set : source_stats_sets_) {
        current_snapshots.stats.push_back(source_stats_set->aggregate());
      }
    }

    ld_debug("Publishing Stats...");
    if (!previous_snapshots.stats.empty()) {
      ld_check_gt(previous_snapshots.stats.size(), 0);
      ld_check_ge(
          current_snapshots.stats.size(), previous_snapshots.stats.size());
      std::vector<const Stats*> previous_stats_sets;
      std::vector<const Stats*> current_stats_sets;

      // Publish only the stats for which we already have a previous value.
      for (int i = 0; i < previous_snapshots.stats.size(); i++) {
        previous_stats_sets.push_back(&previous_snapshots.stats[i]);
        current_stats_sets.push_back(&current_snapshots.stats[i]);
      }

      publisher_->publish(
          current_stats_sets,
          previous_stats_sets,
          duration_cast<milliseconds>(now - previous_snapshots.when));
    }

    previous_snapshots = std::move(current_snapshots);

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

std::unique_ptr<StatsCollectionThread> StatsCollectionThread::maybeCreate(
    const UpdateableSettings<Settings>& settings,
    std::shared_ptr<ServerConfig> config,
    std::shared_ptr<PluginRegistry> plugin_registry,
    int num_shards,
    const StatsHolder* source) {
  ld_check(settings.get());
  ld_check(config);
  ld_check(plugin_registry);

  auto stats_collection_interval = settings->stats_collection_interval;
  if (stats_collection_interval.count() <= 0) {
    return nullptr;
  }

  auto factory = plugin_registry->getSinglePlugin<StatsPublisherFactory>(
      PluginType::STATS_PUBLISHER_FACTORY);
  if (!factory) {
    return nullptr;
  }

  auto stats_publisher = (*factory)(settings, num_shards);
  if (!stats_publisher) {
    return nullptr;
  }

  stats_publisher->addRollupEntity(config->getClusterName());
  return std::make_unique<StatsCollectionThread>(
      source, stats_collection_interval, std::move(stats_publisher));
}

}} // namespace facebook::logdevice
