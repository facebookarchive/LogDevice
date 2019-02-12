/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/PerClientNodeStatsAggregator.h"

#include <folly/stats/BucketedTimeSeries.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace ::testing;

namespace {
const std::chrono::seconds aggregation_period{30};
}

class MockPerClientNodeStatsAggregator : public PerClientNodeStatsAggregator {
 public:
  MockPerClientNodeStatsAggregator() : PerClientNodeStatsAggregator() {
    ON_CALL(*this, getStats()).WillByDefault(Return(&stats));
  }

  MOCK_CONST_METHOD0(getStats, StatsHolder*());
  MOCK_CONST_METHOD0(getWorstClientCount, unsigned int());

  std::chrono::milliseconds getAggregationPeriod() const override {
    return aggregation_period;
  }

 protected:
  StatsHolder stats{
      StatsParams{}.setNodeStatsRetentionTimeOnNodes(DEFAULT_TEST_TIMEOUT)};
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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client, stats_about, 100, 100);

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_1, stats_about, 100, 100);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_2, stats_about, 100, 100);

  EXPECT_EQ(received_stats, aggregator.aggregate(1));
}

TEST(PerClientNodeStatsAggregatorTest, AggregateMultipleNodesSingleClient) {
  MockPerClientNodeStatsAggregator aggregator;
  EXPECT_CALL(aggregator, getWorstClientCount()).WillRepeatedly(Return(0));

  NodeID node_1{1}, node_2{2};
  ClientID client{1};

  auto holder_ptr = aggregator.getStats();

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client, node_1, 200, 50);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client, node_2, 100, 25);

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_1, node_1, 100, 10);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_1, node_2, 200, 20);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_2, node_1, 300, 30);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client_2, node_2, 400, 40);

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

  auto& client_stats = aggregator.getStats()->get().per_client_node_stats;

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client, stats_about, 100, 50);

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client1, stats_about, 100, 50);

  // client 2 is obviously worse
  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client2, stats_about, 25, 200);

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client1, stats_about, 100, 50);

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client2, stats_about, 200, 25);

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

  PER_CLIENT_NODE_STAT_ADD(holder_ptr, client, stats_about, 100, 50);

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

  auto& client_stats = aggregator.getStats()->get().per_client_node_stats;

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
