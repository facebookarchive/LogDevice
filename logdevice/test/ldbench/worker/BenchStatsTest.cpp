/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/BenchStats.h"

#include <vector>

#include <gtest/gtest.h>

namespace facebook { namespace logdevice { namespace ldbench {
class BenchStatsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bench_stats = std::make_unique<BenchStats>("append_sync");
    bench_stats->incStat(StatsType::SUCCESS, 1);
    bench_stats->incStat(StatsType::FAILURE, -1);
  }

  void TearDown() override {}

  std::unique_ptr<BenchStats> bench_stats;
};

TEST_F(BenchStatsTest, updateStatsTest) {
  // collect the stats
  folly::dynamic stats_obj = bench_stats->collectStatsAsPairs();
  EXPECT_EQ(1, stats_obj["success"].asInt());
  EXPECT_EQ(-1, stats_obj["fail"].asInt());
  BenchStats stats_temp("append_sync");
  stats_temp.incStat(StatsType::SUCCESS, 2);
  bench_stats->aggregate(stats_temp);
  folly::dynamic stats_obj2 = bench_stats->collectStatsAsPairs();
  EXPECT_EQ(3, stats_obj2["success"].asInt());
}

}}} // namespace facebook::logdevice::ldbench
