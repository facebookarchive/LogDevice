/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/sequencer_boycotting/BoycottingStats.h"

#include <array>
#include <atomic>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/sequencer_boycotting/PerClientNodeStatsAggregator.h"

using namespace facebook::logdevice;
using namespace ::testing;

namespace {
constexpr auto
    DEFAULT_RETENTION(std::chrono::duration_cast<std::chrono::milliseconds>(
        DEFAULT_TEST_TIMEOUT));
const std::chrono::seconds aggregation_period{30};
} // namespace

/**
 * Creates two maps where the key is a NodeID and the value is the sum of the
 * appends on that node. The maps are called append_success and append_fail.
 * It loops over all threads, clients, and nodes, and sums the stats together
 * into the above mentioned maps
 */
auto perClientNodeStatCollect(BoycottingStatsHolder& holder) {
  std::unordered_map<NodeID, uint32_t, NodeID::Hash> append_success;
  std::unordered_map<NodeID, uint32_t, NodeID::Hash> append_fail;

  const auto now = std::chrono::steady_clock::now();
  holder.runForEach([&](auto& stats) {
    stats.wlock()->updateCurrentTime(now);
    auto total = stats.rlock()->sum();
    for (auto& bucket : total) {
      append_success[bucket.node_id] += bucket.value.successes;
      append_fail[bucket.node_id] += bucket.value.failures;
    }
  });
  return std::make_pair(std::move(append_success), std::move(append_fail));
}

TEST(BoycottingStatsTest, PerClientNodeTimeSeriesStats_SingleClientSimple) {
  BoycottingStatsHolder holder(DEFAULT_RETENTION);

  ClientID client_id{1};
  NodeID node_id{1};

  perClientNodeStatAdd(&holder, client_id, node_id, 10, 5);

  auto [append_success, append_fail] = perClientNodeStatCollect(holder);

  EXPECT_EQ(10, append_success[node_id]);
  EXPECT_EQ(5, append_fail[node_id]);
}

// check that values are summed together
TEST(BoycottingStats, PerClientNodeTimeSeriesStats_SingleClientAppendMany) {
  BoycottingStatsHolder holder(DEFAULT_RETENTION);

  ClientID client_id{1};
  NodeID node_id{1};

  perClientNodeStatAdd(&holder, client_id, node_id, 10, 0);
  perClientNodeStatAdd(&holder, client_id, node_id, 50, 0);
  perClientNodeStatAdd(&holder, client_id, node_id, 2000, 0);

  auto [append_success, append_fail] = perClientNodeStatCollect(holder);

  EXPECT_EQ(2060, append_success[node_id]);
}

TEST(BoycottingStatsTest, PerClientNodeTimeSeriesStats_ManyNodes) {
  BoycottingStatsHolder holder(DEFAULT_RETENTION);

  ClientID client_id{1};
  NodeID node_id_1{1}, node_id_2{2};

  perClientNodeStatAdd(&holder, client_id, node_id_1, 10, 100);

  perClientNodeStatAdd(&holder, client_id, node_id_2, 20, 200);

  auto [append_success, append_fail] = perClientNodeStatCollect(holder);

  EXPECT_EQ(10, append_success[node_id_1]);
  EXPECT_EQ(100, append_fail[node_id_1]);

  EXPECT_EQ(20, append_success[node_id_2]);
  EXPECT_EQ(200, append_fail[node_id_2]);
}

TEST(BoycottingStatsTest, PerClientNodeTimeSeriesStats_ManyClients) {
  BoycottingStatsHolder holder(DEFAULT_RETENTION);

  ClientID client_id_1{1}, client_id_2{2};
  NodeID node_id{1};

  perClientNodeStatAdd(&holder, client_id_1, node_id, 10, 100);

  perClientNodeStatAdd(&holder, client_id_2, node_id, 20, 200);

  auto [append_success, append_fail] = perClientNodeStatCollect(holder);

  EXPECT_EQ(30, append_success[node_id]);
  EXPECT_EQ(300, append_fail[node_id]);
}

/**
 * each thread will execute 100k times, adding 10 to success and 5 to fail,
 * for both clients and nodes
 */
TEST(BoycottingStatsTest, PerClientNodeTimeSeriesStats_ManyThreads) {
  BoycottingStatsHolder holder(DEFAULT_RETENTION);

  ClientID client_id_1{1}, client_id_2{2};
  NodeID node_id_1{1}, node_id_2{2};

  constexpr int nthreads = 8;
  constexpr int per_thread_count = 100000;

  constexpr auto success_expected = per_thread_count * nthreads * 10 * 2;
  constexpr auto fail_expected = per_thread_count * nthreads * 5 * 2;

  std::vector<std::thread> threads;
  Semaphore sem1, sem2;

  for (int i = 0; i < nthreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < per_thread_count; ++j) {
        // client 1, node 1
        perClientNodeStatAdd(&holder, client_id_1, node_id_1, 10, 5);

        // client 1, node 2
        perClientNodeStatAdd(&holder, client_id_1, node_id_2, 10, 5);

        // client 2, node 1
        perClientNodeStatAdd(&holder, client_id_2, node_id_1, 10, 5);

        // client 2, node 2
        perClientNodeStatAdd(&holder, client_id_2, node_id_2, 10, 5);
      }

      sem1.post();

      // have to loop over all threads before exiting thread
      sem2.wait();
    });
  }

  for (int i = 0; i < nthreads; ++i) {
    sem1.wait();
  }

  auto [append_success, append_fail] = perClientNodeStatCollect(holder);

  EXPECT_EQ(success_expected, append_success[node_id_1]);
  EXPECT_EQ(fail_expected, append_fail[node_id_1]);

  EXPECT_EQ(success_expected, append_success[node_id_2]);
  EXPECT_EQ(fail_expected, append_fail[node_id_2]);

  // release all threads
  for (int i = 0; i < nthreads; ++i) {
    sem2.post();
  }
  // make sure that all threads are done before quitting
  for (int i = 0; i < nthreads; ++i) {
    threads[i].join();
  }
}

TEST(BoycottingStatsTest, PerClientNodeTimeSeries_UpdateAggregationTime) {
  std::chrono::seconds initial_retention_time{10};
  ClientID client_id{1};
  NodeID node_id{1};
  PerClientNodeTimeSeriesStats per_client_stats(initial_retention_time);

  // add values to create an time series for that node, to be able to check that
  // the retention time gets updated
  per_client_stats.append(client_id, node_id, 1, 1);

  // check initial retention time
  EXPECT_EQ(initial_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(initial_retention_time, per_client_stats.timeseries()->duration());

  std::chrono::seconds updated_retention_time{50};
  per_client_stats.updateRetentionTime(updated_retention_time);

  // check update retention time
  EXPECT_EQ(updated_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(updated_retention_time, per_client_stats.timeseries()->duration());
}

// if latest_timepoint - earliest_timepoint > retention_time
// caused a crash earlier, make sure to catch this case
TEST(BoycottingStatsTest,
     PerClientNodeTimeSeries_UpdateAggregationTimeDecreaseDuration) {
  const auto initial_retention_time = std::chrono::seconds{50};
  ClientID client_id{1};
  NodeID node_id{1};
  PerClientNodeTimeSeriesStats per_client_stats(initial_retention_time);

  const auto now = std::chrono::steady_clock::now();

  per_client_stats.append(
      client_id, node_id, 1, 1, now - std::chrono::seconds{25});
  per_client_stats.append(client_id, node_id, 1, 1, now);

  // less than latest_timepoint - earliest_timepoint
  const auto updated_retention_time = std::chrono::seconds{10};
  per_client_stats.updateRetentionTime(updated_retention_time);

  // check update retention time
  EXPECT_EQ(updated_retention_time, per_client_stats.retentionTime());
  EXPECT_EQ(updated_retention_time, per_client_stats.timeseries()->duration());
}

class MockPerClientNodeStatsAggregator : public PerClientNodeStatsAggregator {
 public:
  MockPerClientNodeStatsAggregator() : PerClientNodeStatsAggregator() {
    ON_CALL(*this, getStats()).WillByDefault(Return(&stats));
  }

  MOCK_CONST_METHOD0(getStats, BoycottingStatsHolder*());
  MOCK_CONST_METHOD0(getWorstClientCount, unsigned int());

  std::chrono::milliseconds getAggregationPeriod() const override {
    return aggregation_period;
  }

 protected:
  BoycottingStatsHolder stats{
      std::chrono::duration_cast<std::chrono::milliseconds>(
          DEFAULT_TEST_TIMEOUT)};
};

TEST(PerClientNodeStatsAggregatorTest, AggregateSimple) {
  MockPerClientNodeStatsAggregator aggregator;
  // only get the sum
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  ClientID client{1};
  NodeID stats_about{1};

  BucketedNodeStats received_stats;
  received_stats.node_ids.emplace_back(stats_about);
  // 1 node, 1 bucket
  received_stats.summed_counts->resize(boost::extents[1][1]);
  BucketedNodeStats::SummedNodeStats summed;
  summed.client_count = 1;
  summed.successes = 100;
  summed.fails = 100;
  (*received_stats.summed_counts)[0][0] = summed;

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client, stats_about, 100, 100);

  EXPECT_EQ(received_stats, aggregator.aggregate(1));
}
TEST(PerClientNodeStatsAggregatorTest, AggregateNoStats) {
  MockPerClientNodeStatsAggregator aggregator;

  auto stats = aggregator.aggregate(1);
  EXPECT_TRUE(stats.node_ids.empty());
  EXPECT_TRUE(stats.summed_counts->empty());
  EXPECT_TRUE(stats.client_counts->empty());
}

TEST(PerClientNodeStatsAggregatorTest, AggregateMultipleClients) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  ClientID client_1{1}, client_2{2};
  NodeID stats_about{1};

  BucketedNodeStats received_stats;
  received_stats.node_ids.emplace_back(stats_about);

  received_stats.summed_counts->resize(boost::extents[1][1]);
  BucketedNodeStats::SummedNodeStats summed;
  summed.client_count = 2;
  summed.successes = 200;
  summed.fails = 200;
  (*received_stats.summed_counts)[0][0] = summed;

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client_1, stats_about, 100, 100);

  perClientNodeStatAdd(holder_ptr, client_2, stats_about, 100, 100);

  EXPECT_EQ(received_stats, aggregator.aggregate(1));
}

TEST(PerClientNodeStatsAggregatorTest, AggregateMultipleNodesSingleClient) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  NodeID node_1{1}, node_2{2};
  ClientID client{1};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client, node_1, 200, 50);

  perClientNodeStatAdd(holder_ptr, client, node_2, 100, 25);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, UnorderedElementsAre(node_1, node_2));
  EXPECT_EQ(1, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(1, (*result.summed_counts)[1][0].client_count);
  EXPECT_EQ(300,
            (*result.summed_counts)[0][0].successes +
                (*result.summed_counts)[1][0].successes);
  EXPECT_EQ(75,
            (*result.summed_counts)[0][0].fails +
                (*result.summed_counts)[1][0].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateMultipleNodesMultipleClients) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  NodeID node_1{1}, node_2{2};
  ClientID client_1{1}, client_2{2};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client_1, node_1, 100, 10);

  perClientNodeStatAdd(holder_ptr, client_1, node_2, 200, 20);

  perClientNodeStatAdd(holder_ptr, client_2, node_1, 300, 30);

  perClientNodeStatAdd(holder_ptr, client_2, node_2, 400, 40);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, UnorderedElementsAre(node_1, node_2));
  EXPECT_EQ(2, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(2, (*result.summed_counts)[1][0].client_count);
  EXPECT_EQ(1000,
            (*result.summed_counts)[0][0].successes +
                (*result.summed_counts)[1][0].successes);
  EXPECT_EQ(100,
            (*result.summed_counts)[0][0].fails +
                (*result.summed_counts)[1][0].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateMultipleBuckets) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  NodeID node{1};
  ClientID client{1};

  BucketedNodeStats expected;
  expected.node_ids.emplace_back(node);
  expected.summed_counts->resize(boost::extents[1][2]);

  // first (newest) bucket
  (*expected.summed_counts)[0][0].client_count = 1;
  (*expected.summed_counts)[0][0].successes = 100;
  (*expected.summed_counts)[0][0].fails = 50;

  // second bucket
  (*expected.summed_counts)[0][1].client_count = 1;
  (*expected.summed_counts)[0][1].successes = 200;
  (*expected.summed_counts)[0][1].fails = 150;

  auto& client_stats = *aggregator.getStats()->get();

  const auto now = std::chrono::steady_clock::now();
  // set the time to 150% the bucket time ago
  // e.g. 45s if the bucket time is 30s
  const auto bucket_1_time = now -
      decltype(aggregation_period){
          static_cast<long>(1.5 * aggregation_period.count())};

  // should be placed in the second bucket
  client_stats.wlock()->append(client, node, 200, 150, bucket_1_time);

  // first bucket
  client_stats.wlock()->append(client, node, 100, 50, now);

  EXPECT_EQ(expected, aggregator.aggregate(2));
}

TEST(PerClientNodeStatsAggregatorTest, AggregateWithOnlyWorst) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(1));

  ClientID client{1};
  NodeID stats_about{1};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client, stats_about, 100, 50);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, ElementsAre(stats_about));
  // don't add to the sum if they're considered the worst
  EXPECT_EQ(0, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].successes);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].fails);

  EXPECT_EQ(100, (*result.client_counts)[0][0][0].successes);
  EXPECT_EQ(50, (*result.client_counts)[0][0][0].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateWithWorstAndSum) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(1));

  ClientID client1{1}, client2{2};
  NodeID stats_about{1};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client1, stats_about, 100, 50);

  // client 2 is obviously worse
  perClientNodeStatAdd(holder_ptr, client2, stats_about, 25, 200);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, ElementsAre(stats_about));

  EXPECT_EQ(1, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(100, (*result.summed_counts)[0][0].successes);
  EXPECT_EQ(50, (*result.summed_counts)[0][0].fails);

  EXPECT_EQ(25, (*result.client_counts)[0][0][0].successes);
  EXPECT_EQ(200, (*result.client_counts)[0][0][0].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateWithManyWorst) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(2));

  ClientID client1{1}, client2{2};
  NodeID stats_about{1};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client1, stats_about, 100, 50);

  perClientNodeStatAdd(holder_ptr, client2, stats_about, 200, 25);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, ElementsAre(stats_about));

  EXPECT_EQ(0, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].successes);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].fails);

  EXPECT_EQ(300,
            (*result.client_counts)[0][0][0].successes +
                (*result.client_counts)[0][0][1].successes);
  EXPECT_EQ(75,
            (*result.client_counts)[0][0][0].fails +
                (*result.client_counts)[0][0][1].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateWithMoreWorstThanClients) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(2));

  ClientID client{1};
  NodeID stats_about{1};

  auto holder_ptr = aggregator.getStats();

  perClientNodeStatAdd(holder_ptr, client, stats_about, 100, 50);

  auto result = aggregator.aggregate(1);
  EXPECT_THAT(result.node_ids, ElementsAre(stats_about));

  EXPECT_EQ(0, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].successes);
  EXPECT_EQ(0, (*result.summed_counts)[0][0].fails);

  // both will exist, but only one will be filled
  EXPECT_EQ(100,
            (*result.client_counts)[0][0][0].successes +
                (*result.client_counts)[0][0][1].successes);
  EXPECT_EQ(50,
            (*result.client_counts)[0][0][0].fails +
                (*result.client_counts)[0][0][1].fails);
}

TEST(PerClientNodeStatsAggregatorTest, AggregateWithWorstAndSumBuckets) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(1));

  NodeID node{1};
  ClientID client1{1}, client2{2};

  auto& client_stats = *aggregator.getStats()->get();

  const auto now = std::chrono::steady_clock::now();
  // set the time to 150% the bucket time ago
  // e.g. 45s if the bucket time is 30s
  const auto bucket_1_time = now -
      decltype(aggregation_period){
          static_cast<long>(1.5 * aggregation_period.count())};

  /**
   * In the first bucket, client 1 is the worst. In the second bucket, client 2
   * is the worst
   */

  // should be placed in the second (oldest) bucket
  client_stats.wlock()->append(client1, node, 200, 50, bucket_1_time);

  client_stats.wlock()->append(client2, node, 50, 200, bucket_1_time);

  // first bucket
  client_stats.wlock()->append(client1, node, 50, 100, now);

  client_stats.wlock()->append(client2, node, 100, 50, now);

  auto result = aggregator.aggregate(2);
  EXPECT_THAT(result.node_ids, ElementsAre(node));

  EXPECT_EQ(1, (*result.summed_counts)[0][0].client_count);
  EXPECT_EQ(100, (*result.summed_counts)[0][0].successes);
  EXPECT_EQ(50, (*result.summed_counts)[0][0].fails);

  EXPECT_EQ(50, (*result.client_counts)[0][0][0].successes);
  EXPECT_EQ(100, (*result.client_counts)[0][0][0].fails);

  EXPECT_EQ(1, (*result.summed_counts)[0][1].client_count);
  EXPECT_EQ(200, (*result.summed_counts)[0][1].successes);
  EXPECT_EQ(50, (*result.summed_counts)[0][1].fails);

  EXPECT_EQ(50, (*result.client_counts)[0][1][0].successes);
  EXPECT_EQ(200, (*result.client_counts)[0][1][0].fails);
}
