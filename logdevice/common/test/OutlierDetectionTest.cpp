/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/OutlierDetection.h"

#include <gtest/gtest.h>

#include "logdevice/common/ShardID.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)
#define N11 ShardID(11, 0)
#define N12 ShardID(12, 0)
#define N13 ShardID(13, 0)
#define N14 ShardID(14, 0)

namespace facebook { namespace logdevice {

class OutlierDetectionTest
    : public ::testing::TestWithParam<OutlierDetection::Method> {
 public:
  using Sample = std::pair<ShardID, size_t>;
  using Samples = std::vector<Sample>;
  OutlierDetection::Method getMethod() const {
    return GetParam();
  }
};

TEST_P(OutlierDetectionTest, FindOutliers_OnlyOneOutlier) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 54}, {N3, 47}, {N4, 52}, {N5, 55}, {N6, 48},
    {N7, 50}, {N8, 52}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 130}};
  // clang-format on

  // Look for at most two outliers. N14 should be the only outlier.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 3, 1, 0);
  Samples expected = {{N14, 130}};
  ASSERT_EQ(expected, result.outliers);

  // Look for at most 1 outlier. N14 should still be the only outlier.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 1, 0);
  expected = {{N14, 130}};
  ASSERT_EQ(expected, result.outliers);
}

TEST_P(OutlierDetectionTest, FindOutliers_TwoOutliers) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 54}, {N3, 47}, {N4, 52}, {N5, 110}, {N6, 48},
    {N7, 50}, {N8, 52}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 130}};
  // clang-format on

  // Look for at most two outliers. N14,N5 should be the two outliers.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 3, 2, 0);
  Samples expected = {{N14, 130}, {N5, 110}};
  ASSERT_EQ(expected, result.outliers);

  // If we look for at most 3 outliers, we should not be given a third one.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 3, 0);
  expected = {{N14, 130}, {N5, 110}};
  ASSERT_EQ(expected, result.outliers);
}

TEST_P(OutlierDetectionTest, FindOutliers_TwoOutliers2) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 54}, {N3, 47}, {N4, 52}, {N5, 410}, {N6, 48},
    {N7, 50}, {N8, 52}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 130}};
  // clang-format on

  // Look for at most two outliers. N14,N5 should be the two outliers.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 3, 2, 0);
  Samples expected = {{N5, 410}, {N14, 130}};
  ASSERT_EQ(expected, result.outliers);

  // If we look for at most 3 outliers, we should not be given a third one.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 3, 0);
  expected = {{N5, 410}, {N14, 130}};
  ASSERT_EQ(expected, result.outliers);

  // If we look for only 1 outlier, we would want to pick N5 since it's much
  // slower than the second outlier (N14).
  // TODO(T21282553): both RMSE and MAD do not work for this use case and will
  // instead not declare any outlier. This is fine but can be improved in the
  // future.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 1, 0);
  // Expected result if we were to improve the detection
  expected = {{N5, 410}};
  ASSERT_NE(expected, result.outliers);
  // Actual result
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_TwoGroups) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 54}, {N3, 115}, {N4, 52}, {N5, 110}, {N6, 48},
    {N7, 50}, {N8, 120}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 130}};
  // clang-format on

  // Look for at most two outliers. There are clearly two buckets (shards that
  // are around ~50 and shards that are around 120) and the cluster of shards
  // around 120 is bigger than two shards. The algorithm should not detect
  // outliers.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 3, 2, 0);
  Samples expected = {};
  ASSERT_EQ(expected, result.outliers);
  // Same if we allow at most 1 outlier.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 1, 0);
  expected = {};
  ASSERT_EQ(expected, result.outliers);
  // Same if we allow at most 3 outlier.
  result = OutlierDetection::findOutliers(getMethod(), samples, 3, 3, 0);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_ThreeGroupsOnlyWorksWithRMSD) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 113}, {N3, 315}, {N4, 52}, {N5, 110}, {N6, 48},
    {N7, 50}, {N8, 120}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 330}};
  // clang-format on

  // Look for at most two outliers. There are clearly three buckets (~50, ~120,
  // ~330). The bucket made of shards around ~330 has 2 shards. We expect them
  // to be detected outliers.
  // Unfortunately, only the RMSD method marks them as outliers, but not MAD.

  auto result = OutlierDetection::findOutliers(getMethod(), samples, 3, 2, 0);
  if (getMethod() == OutlierDetection::Method::RMSD) {
    Samples expected = {{N14, 330}, {N3, 315}};
    ASSERT_EQ(expected, result.outliers);
  } else {
    ASSERT_TRUE(result.outliers.empty());
  }
}

TEST_P(OutlierDetectionTest, FindOutliers_TwoOutliersButOneFiltered) {
  // clang-format off
  Samples samples{
    {N0, 52}, {N1, 49}, {N2, 54}, {N3, 47}, {N4, 52}, {N5, 110}, {N6, 48},
    {N7, 50}, {N8, 52}, {N9, 50}, {N10, 51}, {N11, 48}, {N12, 53}, {N13, 54},
    {N14, 52}};
  // clang-format on

  // N5 cannot be an outlier.
  std::function<bool(const Sample&)> filter = [=](const Sample& s) {
    return s.first != N5;
  };

  // Look for at most two outliers.
  auto result =
      OutlierDetection::findOutliers(getMethod(), samples, 3, 2, 0, filter);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_AllSame) {
  // clang-format off
  Samples samples{
    {N0, 50}, {N1, 50}, {N2, 50}, {N3, 50}, {N4, 50}, {N5, 50}, {N6, 50},
    {N7, 50}, {N8, 51}, {N9, 50}, {N10, 50}, {N11, 50}, {N12, 50}, {N13, 50},
    {N14, 50}};
  // clang-format on

  // Look for at most two outliers with a sensitivety of 10%.
  // The algorithm should consider N8 as an outlier, but skip it in the end
  // because its latency is not bigger than the average latency (50) by >10%.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 5, 2, 0.1);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_LinearFn) {
  // clang-format off
  Samples samples{
    {N0, 10}, {N1, 20}, {N2, 30}, {N3, 40}, {N4, 50}, {N5, 60}, {N6, 70},
    {N7, 80}, {N8, 90}, {N9, 100}, {N10, 110}, {N11, 120}, {N12, 130},
    {N13, 140}, {N14, 150}};
  // clang-format on

  // Whether we look for 1 or 2 outliers, the result should be empty.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 5, 1, 0);
  ASSERT_TRUE(result.outliers.empty());
  result = OutlierDetection::findOutliers(getMethod(), samples, 5, 2, 0);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_EdgeCaseNoSamples) {
  Samples samples{};
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 5, 1, 0);
  ASSERT_TRUE(result.outliers.empty());
  result = OutlierDetection::findOutliers(getMethod(), samples, 5, 2, 0);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_EdgeCaseOnlyOneSample) {
  Samples samples{{N0, 10}};
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 5, 1, 0);
  ASSERT_TRUE(result.outliers.empty());
  result = OutlierDetection::findOutliers(getMethod(), samples, 5, 2, 0);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_EdgeCaseAllFiltered) {
  Samples samples{{N0, 10}, {N1, 30}, {N2, 40}};
  std::function<bool(const Sample&)> filter = [=](const Sample&) {
    return false;
  };
  auto result =
      OutlierDetection::findOutliers(getMethod(), samples, 5, 1, 0, filter);
  ASSERT_TRUE(result.outliers.empty());
}

TEST_P(OutlierDetectionTest, FindOutliers_EdgeCaseMaxGreaterThanSampleSetSize) {
  Samples samples{{N0, 10}, {N1, 30}, {N2, 40}};
  // Running on such a small sample size does not make sense, the results are
  // not very meaningful. Just check that we don't crash and the algorithm does
  // not mark everyone as an outlier.
  auto result = OutlierDetection::findOutliers(getMethod(), samples, 5, 8, 0);
  ASSERT_NE(3, result.outliers.size());
}

INSTANTIATE_TEST_CASE_P(OutlierDetectionTest,
                        OutlierDetectionTest,
                        ::testing::Values(OutlierDetection::Method::MAD,
                                          OutlierDetection::Method::RMSD));

}} // namespace facebook::logdevice
