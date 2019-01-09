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

std::unique_ptr<StatsCollectionThread> StatsCollectionThread::maybeCreate(
    const UpdateableSettings<Settings>& settings,
    std::shared_ptr<ServerConfig> config,
    std::shared_ptr<PluginRegistry> plugin_registry,
    StatsPublisherScope scope,
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
  auto stats_publisher = (*factory)(scope, settings, num_shards);
  if (!stats_publisher) {
    return nullptr;
  }
  auto rollup_entity = config->getClusterName();
  stats_publisher->addRollupEntity(rollup_entity);
  if (scope == StatsPublisherScope::CLIENT) {
    // This is here for backward compatibility with our tooling. The
    // <tier>.client entity space is deprecated and all new tooling should
    // be using the tier name without suffix
    stats_publisher->addRollupEntity(rollup_entity + ".client");
  }
  return std::make_unique<StatsCollectionThread>(
      source, stats_collection_interval, std::move(stats_publisher));
}

}} // namespace facebook::logdevice
