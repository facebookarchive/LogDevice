/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WorkerTimeoutStats.h"

#include <chrono>

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

using namespace std::chrono_literals;
using namespace std::chrono;
// With balanced load, expect balanced assignment
TEST(WorkerTimeoutStatsTest, DummyTest) {
  WorkerTimeoutStats stats;
  auto overall_result =
      stats.getEstimations(WorkerTimeoutStats::Levels::TEN_SECONDS);
  ASSERT_EQ(overall_result.hasValue(), 1);

  std::array<WorkerTimeoutStats::Latency, WorkerTimeoutStats::kQuantiles.size()>
      canon_result = {{1, 1, 1, 1, 1, 1}};

  // Initially all histograms are 0, so after 2^percentile we will get ones.
  ASSERT_EQ(*overall_result, canon_result);

  ShardID shard_id{1, 1};
  STORE_Header store_hdr{};
  store_hdr.rid = RecordID{1, logid_t{1}};
  store_hdr.wave = 1;

  // Shouldn't update histogram because there wasn't outgoing message.
  stats.onReply(shard_id, store_hdr);
  auto result = stats.getEstimations(
      WorkerTimeoutStats::Levels::TEN_SECONDS, shard_id.node());
  ASSERT_EQ(result.hasValue(), 0); // no histogram
  overall_result =
      stats.getEstimations(WorkerTimeoutStats::Levels::TEN_SECONDS);
  ASSERT_EQ(overall_result.hasValue(), 1);
  ASSERT_EQ(*overall_result, canon_result);

  auto st = steady_clock::now();
  stats.onCopySent(Status::OK, shard_id, store_hdr);
  std::this_thread::sleep_for(2s);
  stats.onReply(shard_id, store_hdr);
  auto took_time = duration_cast<milliseconds>(steady_clock::now() - st);
  overall_result = stats.getEstimations(
      WorkerTimeoutStats::Levels::TEN_SECONDS, shard_id.node());
  ASSERT_EQ(overall_result.hasValue(), 1);
  for (int i = 0; i < 6; ++i) {
    std::cerr << took_time.count() << ' ' << (*overall_result)[i] << '\n';
    ASSERT_LE((*overall_result)[i] - took_time.count(), 5);
  }
}

TEST(WorkerTimeoutStatsTest, StressTest) {
  WorkerTimeoutStats stats;

  ShardID shard_id{1, 1};
  STORE_Header store_hdr{};
  store_hdr.rid = RecordID{1, logid_t{1}};
  store_hdr.wave = 1;

  for (int i = 0; i < 1e6; ++i) {
    stats.onReply(shard_id, store_hdr);
    store_hdr.wave++;
  }

  auto st = steady_clock::now();
  stats.onCopySent(Status::OK, shard_id, store_hdr);
  std::this_thread::sleep_for(2s);
  stats.onReply(shard_id, store_hdr);
  auto took_time = duration_cast<milliseconds>(steady_clock::now() - st);
  auto overall_result = stats.getEstimations(
      WorkerTimeoutStats::Levels::TEN_SECONDS, shard_id.node());
  ASSERT_EQ(overall_result.hasValue(), 1);
  for (int i = 0; i < 6; ++i) {
    std::cerr << took_time.count() << ' ' << (*overall_result)[i] << '\n';
    ASSERT_LE((*overall_result)[i] - took_time.count(), 5);
  }
}

}} // namespace facebook::logdevice
