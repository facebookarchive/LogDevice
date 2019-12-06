/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "logdevice/test/ldbench/worker/BenchStats.h"
#include "logdevice/test/ldbench/worker/FileBasedStatsStore.h"

namespace facebook { namespace logdevice { namespace ldbench {
class BenchStatsHolderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bench_stats_holder = std::make_shared<BenchStatsHolder>("append_sync");
    // update stats with 10 threads
    updateStats();
    // update stats again with another 10 threads
    updateStats();
  }

  void TearDown() override {}

  void updateStats() {
    std::vector<std::thread> update_threads;
    for (int i = 0; i < 10; i++) {
      update_threads.push_back(std::thread([this]() {
        this->bench_stats_holder->getOrCreateTLStats()->incStat(
            StatsType::SUCCESS, 1);
        this->bench_stats_holder->getOrCreateTLStats()->incStat(
            StatsType::FAILURE, -1);
      }));
    }
    for (int i = 0; i < 10; i++) {
      update_threads[i].join();
    }
    return;
  }
  std::shared_ptr<BenchStatsHolder> bench_stats_holder;
};

TEST_F(BenchStatsHolderTest, updateStatsHolderTest) {
  // collect the stats
  folly::dynamic stats_obj = bench_stats_holder->aggregateAllStats();
  uint64_t success_count = stats_obj["success"].asInt();
  EXPECT_EQ(20, success_count);
  uint64_t failed_count = stats_obj["fail"].asInt();
  EXPECT_EQ(-20, failed_count);
}

TEST_F(BenchStatsHolderTest, collectionThreadTest) {
  std::string tmp_file_name = std::tmpnam(nullptr);
  auto store = std::make_shared<FileBasedStatsStore>(tmp_file_name);
  auto collect_thread = std::make_shared<BenchStatsCollectionThread>(
      bench_stats_holder, store, 0);
  updateStats();
  std::ifstream ifs(tmp_file_name, std::ifstream::in);
  std::vector<std::string> stat_strs;
  std::string stat_str;
  collect_thread.reset();
  while (ifs >> stat_str) {
    stat_strs.push_back(stat_str);
  }
  EXPECT_GT(stat_strs.size(), 0);
  auto stats_obj = folly::parseJson(stat_strs.back());
  EXPECT_EQ(stats_obj["success"].asInt(), 30);
  std::remove(tmp_file_name.c_str());
}
}}} // namespace facebook::logdevice::ldbench
