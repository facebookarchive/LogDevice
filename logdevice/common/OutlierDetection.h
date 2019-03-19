/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cmath>
#include <functional>
#include <limits>
#include <numeric>
#include <vector>

#include "logdevice/common/debug.h"
#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice {

class OutlierDetection {
 public:
  enum class Method {
    MAD, // Median-absolute deviation
    RMSD // Root-mean-square deviation
  };

  template <typename Sample>
  struct Result {
    // The set of computed outliers.
    std::vector<Sample> outliers;
    // The center of all samples. Can be mean or median value depending on the
    // method.
    float center;
    // The threshold that was used to consider outliers.
    // If the function found outliers, that is the threshold that was
    // considered. If the function did not find outliers, this is the threshold
    // that would have been required for the top `max_outliers` samples that
    // pass the `outlier_filter` to be considered outliers.
    // If max_outliers is zero or none of the samples pass the `outlier_filter`,
    // this is set to max().
    float threshold;
  };

  /**
   * In a set of positive values, find the ones that are "significantly"
   * greater than others. `Sample` must be a std::pair<K,V> where
   * K is the type of the unique identifier for a sample and V is the type of
   * the value to compare samples against.
   *
   * Intended usage is finding nodes that are unusually slow.
   *
   * In order to find out if the top N samples are outliers, this function
   * computes statistics about the distribution of the supposedly non outlier
   * nodes, and given the `required_margin` and `num_deviations` parameters
   * verifies that the top N samples are above some threshold.
   *
   * This function will try to find as many outliers as allowed by the
   * `max_outliers` parameter.
   *
   * @param samples         Samples within which to find outliers.
   * @param num_deviations  How many deviations from the "center" of the normal
   *                        distribution to expect in order to consider a sample
   *                        an outlier.
   *                        5 should be a good value for most purposes.
   * @param max_outliers    How many outliers at most are allowed.
   * @param required_margin Do not consider samples outliers if they are not
   *                        greater than the average of all samples in the
   *                        normal distribution plus some margin defined by this
   *                        parameter. For instance, if it is equal to 0.7, this
   *                        means outliers are expected to be 70% greater than
   *                        the average.
   * @param outlier_filter A function that given a sample indicates whether it
   *                       is allowed to be considered as an outlier.
   *                       A sample not allowed to be considered an outlier will
   *                       always be taken into account when computing
   *                       statistics about the probability distribution of
   *                       supposedly non outlier samples.
   */
  template <typename Sample>
  static Result<Sample>
  findOutliers(Method method,
               std::vector<Sample> samples,
               float num_deviations,
               size_t max_outliers,
               float required_margin,
               std::function<bool(const Sample&)> outlier_filter = nullptr) {
    ld_assert_ge(required_margin, 0.0);

    Result<Sample> res;
    res.center = std::numeric_limits<float>::max();
    res.threshold = std::numeric_limits<float>::max();

    if (samples.empty()) {
      return res;
    }

    auto bound = samples.end();
    if (outlier_filter) {
      // Put all shards that cannot be outliers at the end of the vector.
      // `bound` is an iterator to the first entry that cannot be an outlier
      // according to the filter or samples.end() if no shard was filtered.
      bound = std::partition(samples.begin(), samples.end(), outlier_filter);
    }

    // Sort the samples that can be made outliers in descending order.
    std::sort(samples.begin(), bound, [](const Sample& a, const Sample& b) {
      return a.second > b.second;
    });

    // Don't try to pick all shards but one as outliers.
    max_outliers = std::min(max_outliers, samples.size() - 1);
    // Don't try to pick a shard that's been filtered out as outlier.
    size_t n_not_filtered = std::distance(samples.begin(), bound);
    max_outliers = std::min(max_outliers, n_not_filtered);

    // Find how many outliers we have.
    int n_outliers = max_outliers;
    while (n_outliers > 0) {
      float threshold_k;
      const bool match = checkTopNSamplesAreOutliers(samples.begin(),
                                                     samples.end(),
                                                     n_outliers,
                                                     method,
                                                     num_deviations,
                                                     required_margin,
                                                     res.center,
                                                     threshold_k);
      res.threshold = std::min(res.threshold, threshold_k);
      if (match) {
        break;
      }
      --n_outliers;
    }

    // Only return the samples that are outliers (if any).
    samples.resize(n_outliers);
    res.outliers = std::move(samples);
    return res;
  }

 private:
  /**
   * Given a list of samples sorted in descending order, check if the top
   * `n_expected_outliers` samples can be considered outliers.
   *
   * This function assumes that all samples but the first `n_expected_outliers`
   * are drawn from the same normal distribution and computes a threshold to
   * compare the top `n_expected_outliers` samples with. The function returns
   * true if the top `n_expected_outliers` samples are above the threshold.
   *
   * @param begin                Beginning of the sorted sequence of samples.
   * @param end                  End of the sequence of samples.
   * @param n_expected_outliers  Number of outliers we are expecting.
   * @param method               @see `computeThreshold`.
   * @param num_deviations       @see `computeThreshold`.
   * @param required_margin      @see `findOutliers`.
   * @param center               This function sets this to the center value of
   *                             the samples. Can be mean or median depending on
   *                             the method.
   * @param threshold            This function sets this to the threshold that
   *                             was used.
   */
  template <typename T>
  static bool checkTopNSamplesAreOutliers(T begin,
                                          T end,
                                          size_t n_expected_outliers,
                                          Method method,
                                          float num_deviations,
                                          float required_margin,
                                          float& center,
                                          float& threshold) {
    ld_assert_lt(n_expected_outliers, std::distance(begin, end));
    auto it = begin + n_expected_outliers;
    threshold = computeThreshold(
        it, end, method, num_deviations, required_margin, center);
    if (threshold != std::numeric_limits<float>::max()) {
      threshold += std::numeric_limits<float>::epsilon();
    }
    auto filter = [&](typename T::reference a) {
      return a.second >= threshold;
    };
    return is_partitioned(begin, end, n_expected_outliers, filter);
  }

  /**
   * Given a set of samples that are assumed to be drawn from the same normal
   * distribution, compute the threshold that should be used for considering
   * other samples outliers.
   *
   * @param begin           Beginning of the sequence of samples.
   * @param end             End of the sequence of samples.
   * @param method          Method for computing a "standard deviation".
   * @param num_deviations  How many "standard deviations" from the "center" are
   *                        needed to consider a sample an outlier.
   *                        Here center may be the mean value or median value
   *                        depending on the method.
   * @param required_margin @see `findOutliers`.
   * @param center          @see `findOutliers`.
   */
  template <typename T>
  static float computeThreshold(T begin,
                                T end,
                                Method method,
                                float num_deviations,
                                float required_margin,
                                float& center) {
    switch (method) {
      case Method::MAD:
        return computeThresholdMAD(
            begin, end, num_deviations, required_margin, center);
      case Method::RMSD:
        return computeThresholdRMSD(
            begin, end, num_deviations, required_margin, center);
    }
    assert(false);
    return std::numeric_limits<float>::max();
  }

  /**
   * Find the average value of a sequence of samples.
   *
   * @param begin Beginning of the sequence of samples.
   * @param end   Beginning of the sequence of samples.
   * @return      Average value in the second.
   */
  template <typename T>
  static float avg(T begin, T end) {
    float sum = 0;
    for (auto it = begin; it != end; ++it) {
      sum += it->second;
    }
    assert(std::distance(begin, end) != 0);
    return sum / std::distance(begin, end);
  }

  /**
   * Find whether or not the given sequence of samples is partitioned such that
   * the first partition passes a filter and the second does not.
   *
   * @param begin  Beginning of the sequence of samples.
   * @param end    End of the sequence of samples.
   * @param sz     How many samples are expected to pass the filter.
   * @param filter Filter to be applied.
   */
  template <typename T, typename F>
  static bool is_partitioned(T begin, T end, size_t sz, F filter) {
    return std::all_of(begin, begin + sz, filter) &&
        std::none_of(begin + sz, end, filter);
  }

  /**
   * Compute the median value in a sorted sequence.
   *
   * @param begin Beginning of the sequence.
   * @param end   End of the sequence.
   * @param f     Function to map an entry in the sequence to the corresponding
   *              value.
   */
  template <typename T, typename F>
  static float median(T begin, T end, F f) {
    size_t distance = std::distance(begin, end);
    assert(distance != 0);
    size_t middle = distance / 2.0;
    if (distance % 2) {
      return f(begin + middle);
    } else {
      return (f(begin + middle - 1) + f(begin + middle)) / 2.0;
    }
  }

  /**
   * Compute the threshold for outlier detection using MAD (Median Absolute
   * Deviation).
   *
   * @param begin           Beginning of the sequence of samples.
   * @param end             Beginning of the sequence of samples.
   * @param num_deviations  How many deviations from the median are required to
   *                        be above the threshold.
   * @param required_margin @see `findOutliers`.
   * @param center          @see `findOutliers`.
   */
  template <typename T>
  static float computeThresholdMAD(T begin,
                                   T end,
                                   float num_deviations,
                                   float required_margin,
                                   float& center) {
    // Compute the median (data is already sorted)
    ld_assert(begin != end);
    center = median(begin, end, [](const T& it) { return it->second; });

    // Compute the absolute deviations to median
    std::vector<float> absolute_dev;
    absolute_dev.reserve(std::distance(begin, end));
    for (auto it = begin; it != end; ++it) {
      absolute_dev.push_back(std::abs(it->second - center));
    }
    std::sort(absolute_dev.begin(), absolute_dev.end());

    // Find the median absolute deviation
    float mad =
        median(absolute_dev.begin(),
               absolute_dev.end(),
               [](std::vector<float>::const_iterator it) { return *it; });
    return center + num_deviations * mad + center * required_margin;
  }

  /**
   * Compute the threshold for outlier detection using RMSD (Root Mean Square
   * Deviation).
   *
   * @param begin           Beginning of the sequence of samples.
   * @param end             Beginning of the sequence of samples.
   * @param num_deviations  How many deviations from the mean are required to
   *                        be above the threshold.
   * @param required_margin @see `findOutliers`.
   * @param center          @see `findOutliers`.
   */
  template <typename T>
  static float computeThresholdRMSD(T begin,
                                    T end,
                                    float num_deviations,
                                    float required_margin,
                                    float& center) {
    assert(std::distance(begin, end) != 0);
    center = avg(begin, end);
    auto acc = [&](float s, typename T::reference x) {
      return s + std::pow(x.second - center, 2);
    };
    using pair_type = typename std::iterator_traits<T>::value_type;
    using val_type = typename std::tuple_element<1, pair_type>::type;
    float sum = std::accumulate(begin, end, val_type{}, acc);
    float rmsd = std::sqrt(sum / std::distance(begin, end));
    return center + num_deviations * rmsd + center * required_margin;
  }
};

}} // namespace facebook::logdevice
