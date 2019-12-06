/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/BenchStats.h"

#include <functional>
#include <iostream>

namespace facebook { namespace logdevice { namespace ldbench {

BenchStats::BenchStats(const std::string& type)
    : success_(0),
      success_byte_(0),
      in_flight_(0),
      failure_(0),
      skipped_(0),
      type_(type){};
BenchStats::~BenchStats(){};

int64_t BenchStats::getAttr(StatsType attr) const {
  switch (attr) {
    case StatsType::SUCCESS:
      return success_;
    case StatsType::SUCCESS_BYTE:
      return success_byte_;
    case StatsType::FAILURE:
      return failure_;
    case StatsType::SKIPPED:
      return skipped_;
    case StatsType::INFLIGHT:
      return in_flight_;
  }
}

void BenchStats::incStat(StatsType attr, int64_t num) {
  switch (attr) {
    case StatsType::SUCCESS:
      success_ += num;
      break;
    case StatsType::SUCCESS_BYTE:
      success_byte_ += num;
      break;
    case StatsType::FAILURE:
      failure_ += num;
      break;
    case StatsType::SKIPPED:
      skipped_ += num;
      break;
    case StatsType::INFLIGHT:
      in_flight_ += num;
      break;
    default:
      break;
  }
}

void BenchStats::aggregate(const BenchStats& stats) {
  success_ += stats.getAttr(StatsType::SUCCESS);
  success_byte_ += stats.getAttr(StatsType::SUCCESS_BYTE);
  failure_ += stats.getAttr(StatsType::FAILURE);
  skipped_ += stats.getAttr(StatsType::SKIPPED);
  in_flight_ += stats.getAttr(StatsType::INFLIGHT);
  return;
}

void BenchStats::display() const {
  std::cout << success_ << " successful, " << success_byte_ << " byte,"
            << skipped_ << " skipped, " << in_flight_ << " inflights, "
            << failure_ << " failed, " << std::endl;
}

folly::dynamic BenchStats::collectStatsAsPairs() {
  folly::dynamic stats_res = folly::dynamic::object();
  stats_res["type"] = type_;
  stats_res["success"] = success_.load();
  stats_res["success_byte"] = success_byte_.load();
  stats_res["fail"] = failure_.load();
  stats_res["skipped"] = skipped_.load();
  stats_res["inflight"] = in_flight_.load();
  return stats_res;
}

BenchStats* BenchStatsHolder::getOrCreateTLStats() {
  BenchStatsWrapper* bench_stat_wrapper = bench_thread_stats_.get();
  if (!bench_stat_wrapper) {
    bench_stat_wrapper = new BenchStatsWrapper(this);
    bench_thread_stats_.reset(bench_stat_wrapper);
  }
  return &bench_stat_wrapper->stats_;
}

void BenchStatsHolder::displayAllForDebug() {
  std::cout << "bench_dead_stats:" << std::endl;
  aggregated_stats_.display();
  for (const auto& stat_wrapper : bench_thread_stats_.accessAllThreads()) {
    stat_wrapper.stats_.display();
  };
}

folly::dynamic BenchStatsHolder::aggregateAllStats() {
  BenchStats result(name_);
  {
    std::lock_guard<std::mutex> lock(aggregated_stats_mutex_);
    result.aggregate(aggregated_stats_);
    for (auto& x : bench_thread_stats_.accessAllThreads()) {
      result.aggregate(x.stats_);
    }
  }
  return result.collectStatsAsPairs();
}

BenchStatsCollectionThread::BenchStatsCollectionThread(
    std::shared_ptr<BenchStatsHolder> stats_source,
    std::shared_ptr<StatsStore> stats_store,
    uint64_t interval)
    : stats_source_(std::move(stats_source)),
      stats_store_(std::move(stats_store)),
      interval_(interval) {
  function_scheduler_.addFunction(
      std::bind(&BenchStatsCollectionThread::statsCollectionFunction, this),
      std::chrono::seconds(interval),
      "publish_stats");
  function_scheduler_.start();
}

BenchStatsCollectionThread::~BenchStatsCollectionThread() {
  function_scheduler_.cancelFunction("publish_stats");
  function_scheduler_.shutdown();
  // We write one more time otherwise will miss some stats based on testing
  statsCollectionFunction();
  return;
}

void BenchStatsCollectionThread::statsCollectionFunction() {
  auto cur_stats = stats_source_->aggregateAllStats();
  cur_stats["timestamp"] =
      std::chrono::system_clock::now().time_since_epoch().count();
  stats_store_->writeCurrentStats(cur_stats);
  return;
}

}}} // namespace facebook::logdevice::ldbench
