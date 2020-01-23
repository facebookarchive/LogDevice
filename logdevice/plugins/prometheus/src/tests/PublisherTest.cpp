/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * @author Mohamed Bassem
 */

#include <gtest/gtest.h>

#include <chrono>

#include "../PrometheusStatsPublisher.h"

#include <logdevice/common/stats/Stats.h>
#include <logdevice/common/toString.h>

namespace facebook { namespace logdevice {


namespace {
  std::ostream& operator<<(std::ostream& os, const prometheus::ClientMetric::Label& l) {
    os << "{" << l.name << ", " << l.value << "}";
    return os;
  }
}


const prometheus::MetricFamily* findFamily(
    const std::vector<prometheus::MetricFamily>& families,
    const std::string& name) {
  for (const auto& f: families) {
    if (f.name == name) {
      return &f;
    }
  }
  return nullptr;
}

const prometheus::ClientMetric* findMetric(
    const prometheus::MetricFamily& family,
    std::vector<prometheus::ClientMetric::Label> labels) {

  sort(labels.begin(), labels.end());

  for (const auto& metric: family.metric) {
    auto mlabel = metric.label;
    sort(mlabel.begin(), mlabel.end());
    if (labels == mlabel) {
      return &metric;
    }
  }
  return nullptr;
}

TEST(PublisherTest, testBasicMetrics) {
  auto registry = std::make_shared<prometheus::Registry>();
  PrometheusStatsPublisher publisher(registry);

  StatsParams params;
  params.is_server = true;
  StatsHolder holder(params);

  STAT_INCR(&holder, post_request_total);
  MESSAGE_TYPE_STAT_INCR(&holder, MessageType::HELLO, message_sent);
  TRAFFIC_CLASS_STAT_INCR((&holder), TrafficClass::READ_TAIL, messages_sent);
  PER_SHARD_STAT_INCR(&holder, shard_dirty, shard_index_t{5});
  REQUEST_TYPE_STAT_INCR(&holder, RequestType::APPEND, post_request);

  auto stats = holder.aggregate();

  publisher.publish(
      std::vector<const Stats*>{&stats},
      std::vector<const Stats*>{nullptr} /* not used */,
      std::chrono::seconds(60));

  auto metrics = registry->Collect();

  {
    // Basic
    auto family = findFamily(metrics, "post_request_total");
    ASSERT_NE(nullptr, family);
    auto metric = findMetric(*family, {{"source", "server"}});
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ(1, metric->gauge.value);
  }

  {
    // Per message
    auto family = findFamily(metrics, "message_sent");
    ASSERT_NE(nullptr, family);
    auto metric = findMetric(*family, {{"source", "server"}, {"message_type", "HELLO"}});
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ(1, metric->gauge.value);
  }

  {
    // Per traffic class
    auto family = findFamily(metrics, "messages_sent");
    ASSERT_NE(nullptr, family);
    auto metric = findMetric(*family, {{"source", "server"}, {"traffic_class", "READ_TAIL"}});
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ(1, metric->gauge.value);
  }

  {
    // Per shard
    auto family = findFamily(metrics, "shard_dirty");
    ASSERT_NE(nullptr, family);
    auto metric = findMetric(*family, {{"source", "server"}, {"shard_index", "5"}});
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ(1, metric->gauge.value);
  }

  {
    // Per request
    auto family = findFamily(metrics, "post_request");
    ASSERT_NE(nullptr, family);
    auto metric = findMetric(*family, {{"source", "server"}, {"request_type", "APPEND"}});
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ(1, metric->gauge.value);
  }
}

}}
