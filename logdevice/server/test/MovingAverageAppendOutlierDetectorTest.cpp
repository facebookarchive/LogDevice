/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/MovingAverageAppendOutlierDetector.h"

#include <random>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace std::literals::chrono_literals;
using namespace ::testing;

namespace {
static constexpr std::chrono::seconds COLLECTION_PERIOD{30};

class MockOutlierDetector : public MovingAverageAppendOutlierDetector {
 private:
  bool use_rmsd_;

 public:
  MockOutlierDetector(bool use_new_outlier_detection)
      : use_rmsd_(use_new_outlier_detection) {
    ON_CALL(*this, getGracePeriod()).WillByDefault(Return(0s));
    ON_CALL(*this, getSensitivity()).WillByDefault(Return(0));
    ON_CALL(*this, getStatsRetentionDuration()).WillByDefault(Return(5min));
  }

  MOCK_CONST_METHOD0(getGracePeriod, std::chrono::milliseconds());
  MOCK_CONST_METHOD0(getSensitivity, float());
  MOCK_CONST_METHOD0(getStatsRetentionDuration, std::chrono::milliseconds());

  std::chrono::milliseconds getStatsCollectionPeriod() const override {
    return COLLECTION_PERIOD;
  }

  double getRelativeMargin() const override {
    return 0.15;
  }

  bool useRMSD() const override {
    return use_rmsd_;
  }

  unsigned int getMaxBoycottCount() const override {
    return 3;
  }

  // have a high sensitivity to not require as many nodes and append counts for
  // a node to be considered an outlier
  double getRequiredStdFromMean() const override {
    return 1;
  }
};

class MovingAverageAppendOutlierDetectorTest
    : public Test,
      public ::testing::WithParamInterface<bool /*use-rmsd*/> {
 public:
  void SetUp() override {
    now = std::chrono::steady_clock::now();
  }

  /**
   * A helper function to always add a certain user specified stats while making
   * sure that enough stats are added to make certain that the mean is close to
   * 1 and the STD is close to 0, as it will be in real clusters
   */
  void
  addManyStats(std::initializer_list<AppendOutlierDetector::NodeStats> start,
               std::chrono::steady_clock::time_point when,
               uint32_t success_count = 100,
               uint32_t fail_count = 0,
               int node_count = 10) {
    std::vector<AppendOutlierDetector::NodeStats> stats(
        node_count, {success_count, fail_count});
    std::copy(start.begin(), start.end(), stats.begin());
    for (size_t i = 0; i < stats.size(); ++i) {
      detector.addStats(i, stats[i], when);
    }
  }

  std::chrono::steady_clock::time_point now;
  MockOutlierDetector detector{GetParam()};
};
} // namespace

TEST_P(MovingAverageAppendOutlierDetectorTest, EmptyStats) {
  EXPECT_THAT(detector.detectOutliers(now), IsEmpty());
}

// make sure not to do divide by zero anywhere
TEST_P(MovingAverageAppendOutlierDetectorTest, ZeroValues) {
  detector.addStats(0, {0, 0}, now);
  EXPECT_THAT(detector.detectOutliers(now), IsEmpty());
}

TEST_P(MovingAverageAppendOutlierDetectorTest, NoGracePeriod) {
  EXPECT_CALL(detector, getGracePeriod()).WillRepeatedly(Return(0s));

  addManyStats({{100, 1}}, now);
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0));
}

TEST_P(MovingAverageAppendOutlierDetectorTest, WithGracePeriod) {
  EXPECT_CALL(detector, getGracePeriod())
      .WillRepeatedly(Return(2 * COLLECTION_PERIOD));

  addManyStats({{100, 1}}, now - 2 * COLLECTION_PERIOD);
  EXPECT_THAT(detector.detectOutliers(now - 2 * COLLECTION_PERIOD), IsEmpty());
  addManyStats({{100, 1}}, now - 1 * COLLECTION_PERIOD);
  EXPECT_THAT(detector.detectOutliers(now - 1 * COLLECTION_PERIOD), IsEmpty());

  addManyStats({{100, 1}}, now);
  // it's now been a potential outlier for 2 periods, should be considered a
  // true outlier
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0));

  // even if stats are added after the grace period is over, it's still an
  // outlier
  addManyStats({{100, 1}}, now + COLLECTION_PERIOD);
  EXPECT_THAT(detector.detectOutliers(now + COLLECTION_PERIOD), ElementsAre(0));

  // but as soon as it's no longer a potential outlier, its status as a true
  // outlier is also removed
  addManyStats({}, now + 2 * COLLECTION_PERIOD);
  EXPECT_THAT(detector.detectOutliers(now + 2 * COLLECTION_PERIOD), IsEmpty());
}

TEST_P(MovingAverageAppendOutlierDetectorTest, NoSensitivity) {
  EXPECT_CALL(detector, getSensitivity()).WillRepeatedly(Return(0.0));

  addManyStats({{100, 1}}, now);
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0));
}

TEST_P(MovingAverageAppendOutlierDetectorTest, WithSensitivity) {
  EXPECT_CALL(detector, getSensitivity())
      .WillRepeatedly(Return(0.015)); // allow 1.5% failure rate

  const bool use_rmsd = GetParam();
  if (use_rmsd) {
    // 1% failure rate and 2% failure rate. With RMSD, no one is deemed outlier.
    addManyStats({{99, 1}, {98, 2}}, now);
    EXPECT_THAT(detector.detectOutliers(now), IsEmpty());
  } else {
    // 1% failure rate and 2% failure rate. Only the second one should fail
    addManyStats({{99, 1}, {98, 2}}, now);
    EXPECT_THAT(detector.detectOutliers(now), ElementsAre(1));
  }
}

TEST_P(MovingAverageAppendOutlierDetectorTest, OrderedByWorst) {
  addManyStats({{99, 1}, {98, 2}}, now);
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(1, 0));

  addManyStats({{96, 4}, {98, 2}}, now);
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0, 1));

  addManyStats({{98, 2}, {98, 2}}, now);
  // node 0 is still worse when considering the mean success ratio.
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0, 1));
}

TEST_P(MovingAverageAppendOutlierDetectorTest, SettingsUpdate) {
  EXPECT_CALL(detector, getStatsRetentionDuration())
      .WillRepeatedly(Return(5min));

  addManyStats({{99, 1}}, now);
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0));

  EXPECT_CALL(detector, getStatsRetentionDuration())
      .WillRepeatedly(Return(10min));

  // even if we update the setting, should not make a difference in this case
  EXPECT_THAT(detector.detectOutliers(now), ElementsAre(0));
}

INSTANTIATE_TEST_CASE_P(MovingAverageAppendOutlierDetectorTest,
                        MovingAverageAppendOutlierDetectorTest,
                        ::testing::Bool());
