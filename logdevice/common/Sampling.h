/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <initializer_list>

#include <folly/Random.h>
#include <folly/small_vector.h>

#include "logdevice/common/Random.h"
#include "logdevice/common/SmallMap.h"
#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

// Allows sampling an item from the distribution in O(log n) time,
// or k distinct items in O(k log n) time (using systematic sampling).

namespace Sampling {
// Requirements for a sample. The sampling is essentially the same regardless
// of the mode; the mode mostly affects handling of overweight items.
enum class Mode {
  // All sampled items will be distinct.
  // If there are overweight items, result can be Result::BIASED.
  // If there are too few nonzero weights, Result::IMPOSSIBLE.
  AT_MOST_ONCE,
  // Every item in 0..size()-1 with nonzero weight will occur at least once
  // in the sample. If not all items with nonzero weight are overweight,
  // result can be Result::BIASED.
  // If there are too many nonzero weights, Result::IMPOSSIBLE.
  AT_LEAST_ONCE,
  // Neither of the two is guaranteed.
  // Fails only if all weights are zero. Never biased.
  WHATEVS,
};

enum class Result {
  OK,
  // Satisfied the Mode, but the sample doesn't come from
  // the right distribution
  // (i.e. probabilities are not proportional to weights).
  BIASED,
  // Can't satisfy Mode because size() is too small or too big.
  IMPOSSIBLE,
};

// An arbitrary threshold for comparing floating-point numbers for equality.
//
// A note on usage of EPSILON in Sampling and WeightedCopySetSelector:
// We want the algorithms to be insensitive to scale of the input numbers.
// E.g. if you multiply weights of all nodes by a trillion (e.g. switch from
// expressing weights in terabytes to bytes), it shouldn't change the behavior
// of copyset selector.
// To achieve that, we use EPSILON as the threshold for either for _relative_
// difference between numbers, or for difference normalized by total weight
// or something like that. E.g. maybe instead of `fabs(x - y) < EPSILON` you'd
// do `fabs(x / y - 1) < EPSILON` if y is never close to zero, or maybe
// `fabs(x - y) < total_weight_of_nodes_in_the_cluster * EPSILON`
constexpr double EPSILON = 1e-9;

// The sampling functions are templated by the probability distribution
// class D, which must have the following methods:
//  * size_t size(),
//  * double prefixSum(size_t count),
//  * double weight(size_t idx),
//  * size_t findPrefixBySum(double s).
// See ProbabilityDistribution for documentation on these methods.
//
// Items with zero weight are never picked.
// Complexity is O(count * log n) in typical cases, but worst case is
// O(n log n).

template <typename D>
Result sampleOne(const D& distribution,
                 size_t* out_idx,
                 RNG& rng = DefaultRNG::get());

// Puts the result in *out, *(out + 1), ..., *(out + (count - 1)).
// The result is sorted.
// O(count * log n) if result is OK; otherwise up to O(n log n).
// @param out_size
//  If out_size != nullptr, and mode = AT_MOST_ONCE, and result is IMPOSSIBLE,
//  *out_size will contain the number of nonzero items, and
//  out[0..*out_size-1] will contain the items themselves.
// @param out_over_or_underweight
//  If mode is AT_MOST_ONCE, and result is BIASED, it means that some item's
//  weight is too big. In this case this item will be assigned to
//  *out_over_or_underweight. If there are multiple such items, an arbitrary
//  one will be used. Similarly, if mode is AT_LEAST_ONCE, an item whose
//  weight is too small will be assigned here.
template <typename D, typename T>
Result sampleMulti(const D& distribution,
                   size_t count,
                   Mode mode,
                   T* out,
                   RNG& rng = DefaultRNG::get(),
                   size_t* out_size = nullptr,
                   T* out_over_or_underweight = nullptr);
} // namespace Sampling

// Representation of an non-normalized discrete probability distribution.
// In other words, an array of weights.
class ProbabilityDistribution {
 public:
  ProbabilityDistribution() = default;
  explicit ProbabilityDistribution(std::initializer_list<double> il)
      : ProbabilityDistribution(il.begin(), il.end()) {}

  template <typename It>
  ProbabilityDistribution(It begin, It end) {
    assign(begin, end);
  }

  size_t size() const {
    return weight_cumsum_.size();
  }

  template <typename It>
  void assign(It begin, It end);
  void assign(std::initializer_list<double> il);

  void push_back(double weight);

  double weight(size_t idx) const;

  // Sum of the first this many weights.
  // prefixSum(i + 1) - prefixSum(i) == weight(i) must hold for all valid i
  // ("==" is up to rounding errors).
  // For convenience,
  //  * prefixSum(0) doesn't have to be zero,
  //  * prefixSum(size()) doesn't have to be one, i.e. the distribution can
  //    be unnormalized. In particular, prefixSum(size()) - prefixSum(0)
  //    may be zero, in which case we can't sample anything from this
  //    distribution.
  double prefixSum(size_t count) const;
  // Finds i such that prefixSum(i) <= s < prefixSum(i+1).
  // If prefixSum(size()) <= s, returns size().
  // 0 <= i <= size().
  // Assumes that all values are nonnegative. If s < 0, returns 0.
  // Note: it may seem that the definition of i implies that weight at index i
  // is positive; however, because of rounding errors it might be possible
  // for it to be zero.
  size_t findPrefixBySum(double s) const;

  double totalWeight() const;

  // See totalUnadjustedWeight() in AdjustedProbabilityDistribution.
  double totalUnadjustedWeight() const {
    return totalWeight();
  }

  std::string toString() const;

 private:
  std::vector<double> weight_cumsum_;

  friend class AdjustedProbabilityDistribution;
};

// ProbabilityDistribution doesn't allow changing individual weights faster
// than in O(n), and it also doesn't allow changing weights in a thread-safe
// way without locking. The next pair of classes addresses these limitations.
//
// ProbabilityDistributionAdjustment represents a sparse set of updates to
// weights of a ProbabilityDistribution. Its memory usage is proportional to
// the number of updated weights.
//
// AdjustedProbabilityDistribution combines a ProbabilityDistributionAdjustment
// and the ProbabilityDistribution to which it should be applied.
// AdjustedProbabilityDistribution behaves like a probability distribution;
// you can use it with sampleOne() and sampleMulti().
//
// Intended use case is: a big immutable ProbabilityDistribution shared among
// threads, a small temporary ProbabilityDistributionAdjustment in each
// thread, and AdjustedProbabilityDistribution combining the two into a
// samplable distribution. The adjustment is used for zeroing out some
// weights of the big distribution (e.g. for blacklisting unavailable nodes).

class ProbabilityDistributionAdjustment {
 public:
  ProbabilityDistributionAdjustment() = default;

  // How many weights are adjusted.
  size_t numUpdatedWeights() const {
    return added_cumsum_.size();
  }
  // True if the weight at given index was adjusted, i.e. if addWeight() has
  // been called and revert() hasn't.
  bool isWeightUpdated(size_t idx) {
    return added_cumsum_.count(idx) != 0;
  }

  // O(numUpdatedWeights()). `delta` can be negative.
  void addWeight(size_t idx, double delta);
  // O(numUpdatedWeights()).
  // Returns the added weight that this idx used to have.
  double revert(size_t idx);

  void clear();

  // O(log numUpdatedWeights()).
  double prefixSum(size_t count) const;
  double weight(size_t idx) const;

  // O(1).
  double totalAddedWeight() const;

 private:
  // Value for key i is the sum of all adjustments for items <= i.
  SmallOrderedMap<size_t, double> added_cumsum_;

  friend class AdjustedProbabilityDistribution;
};

// Owns neither the distribution nor the adjustment, just has two pointers.
// Copyable and lightweight.
class AdjustedProbabilityDistribution {
 public:
  explicit AdjustedProbabilityDistribution(
      const ProbabilityDistribution* base,
      const ProbabilityDistributionAdjustment* diff = nullptr)
      : base_(base), diff_(diff) {}

  size_t size() const {
    return base_->size();
  }

  double prefixSum(size_t count) const;
  double weight(size_t idx) const;
  size_t findPrefixBySum(double s) const;

  // O(1).
  double totalWeight() const;

  // Total weight before applying adjustments.
  // Used as a scaling factor when comparing adjusted weights to zero
  // using EPSILON.
  // Example: when sampling from adjusted distribution {1 - 0.99, 2 - 0}, it
  // makes sense to consider the first 0.01 weight "nonzero", but when sampling
  // from {1e12 - (1e12-0.01), 2e12}, the first 0.01 weight is probably
  // a rounding error and should be considered zero.
  // Unadjusted weight is better for this purpose because it works even if all
  // weights are adjusted to zero and all adjusted weights are rounding error.
  double totalUnadjustedWeight() const {
    return base_->totalWeight();
  }

  std::string toString() const;

 private:
  const ProbabilityDistribution* base_;
  const ProbabilityDistributionAdjustment* diff_;
};

// Implementation of template methods below.

template <typename It>
void ProbabilityDistribution::assign(It begin, It end) {
  weight_cumsum_.assign(begin, end);
  for (double x : weight_cumsum_) {
    ld_check(x >= 0);
  }
  std::partial_sum(
      weight_cumsum_.begin(), weight_cumsum_.end(), weight_cumsum_.begin());
}

namespace Sampling {

template <typename D>
Sampling::Result sampleOne(const D& distribution, size_t* out_idx, RNG& rng) {
  ld_check(out_idx != nullptr);
  size_t size = distribution.size();
  double sum_begin = distribution.prefixSum(0);
  double sum_end = distribution.prefixSum(size);
  double total_weight = sum_end - sum_begin;
  const double scaled_epsilon = EPSILON * distribution.totalUnadjustedWeight();
  ld_check(total_weight >= -scaled_epsilon);
  if (total_weight <= scaled_epsilon) {
    return Result::IMPOSSIBLE;
  }
  ld_check(size > 0);
  double point = sum_begin + total_weight * folly::Random::randDouble01(rng);
  size_t x = distribution.findPrefixBySum(point);
  if (x < size && distribution.weight(x) > scaled_epsilon) {
    // The most likely case.
    *out_idx = x;
    return Result::OK;
  }
  // We've hit some zero-weight item, presumably because of rounding errors.
  // Let's find a positive-weight item to return.
  for (size_t steps = 0; steps < size; ++steps) {
    if (distribution.weight((x + steps) % size) > scaled_epsilon) {
      *out_idx = x;
      return Result::OK;
    }
  }
  ld_check(false);
  return Result::IMPOSSIBLE;
}

template <typename D, typename T>
Sampling::Result sampleMulti(const D& distribution,
                             size_t count,
                             Mode mode,
                             T* out,
                             RNG& rng,
                             size_t* out_size_ptr,
                             T* out_over_or_underweight) {
  // This code consists mostly of handling of various error conditions and
  // corner cases. Here's a clean less robust version, easier to understand:
  //   double s0 = distribution.prefixSum(0);
  //   double s1 = distribution.prefixSum(distribution.size());
  //   double step = (s1 - s0) / count;
  //   double point = s0 + rand01() * step;
  //   for (size_t i = 0; i < count; ++i) {
  //     out[i] = distribution.findPrefixBySum(point);
  //     point += step;
  //   }
  // Now that you understand how this function essentially works, let's get to
  // the ugly details.

  size_t out_size = 0;
  SCOPE_EXIT {
    if (out_size_ptr) {
      *out_size_ptr = out_size;
    }
  };

  if (count == 0) {
    return Result::OK;
  }
  ld_check(out != nullptr);

  size_t size = distribution.size();
  double sum_begin = distribution.prefixSum(0);
  double sum_end = distribution.prefixSum(size);
  double total_weight = sum_end - sum_begin;
  double scaled_epsilon = EPSILON * distribution.totalUnadjustedWeight();

  ld_check(total_weight >= -scaled_epsilon);
  if (total_weight <= scaled_epsilon) {
    return Result::IMPOSSIBLE;
  }
  ld_check(size != 0);

  double step = total_weight / count;
  double point = sum_begin + step * folly::Random::randDouble01(rng);

  size_t prev_x = -1;
  double prev_sum = sum_begin;
  bool missed = false;
  for (size_t step_idx = 0; step_idx < count; ++step_idx) {
    size_t x = distribution.findPrefixBySum(point);
    point += step;

    if (x == size || x + 1 < prev_x + 1) {
      continue;
    }

    double weight = distribution.weight(x);
    if (weight <= scaled_epsilon) {
      ld_check(weight >= -scaled_epsilon);
      continue;
    }

    if (mode == Mode::AT_MOST_ONCE && x == prev_x) {
      if (out_over_or_underweight) {
        *out_over_or_underweight = static_cast<T>(x);
      }
      continue;
    }

    if (mode == Mode::AT_LEAST_ONCE) {
      double cur_sum = distribution.prefixSum(x);
      if (cur_sum - prev_sum > scaled_epsilon) {
        // Missed some item.
        missed = true;
      }
      prev_sum = cur_sum + weight;
    }

    out[out_size++] = static_cast<T>(x);
    prev_x = x;
  }

  if (mode == Mode::AT_LEAST_ONCE && sum_end - prev_sum > scaled_epsilon) {
    missed = true;
  }

  if (out_size == count && !missed) {
    return Result::OK;
  }

  if (mode == Mode::AT_LEAST_ONCE) {
    folly::small_vector<T, 10> excess;
    for (size_t i = 1; i < out_size; ++i) {
      if (out[i] == out[i - 1]) {
        excess.push_back(out[i]);
      }
    }

    out_size = 0;
    bool found_underweight = false;
    for (size_t x = 0; x < size; ++x) {
      double weight = distribution.weight(x);
      if (weight <= scaled_epsilon) {
        ld_check(weight >= -scaled_epsilon);
        continue;
      }
      if (out_size == count) {
        return Result::IMPOSSIBLE;
      }
      if (weight < step - scaled_epsilon) {
        if (out_over_or_underweight) {
          *out_over_or_underweight = static_cast<T>(x);
        }
        found_underweight = true;
      }
      out[out_size++] = static_cast<T>(x);
    }
    ld_check(found_underweight);

    if (out_size < count && !excess.empty()) {
      std::rotate(
          excess.begin(), excess.begin() + rng() % excess.size(), excess.end());
      size_t to_copy = std::min(excess.size(), count - out_size);
      std::copy(excess.begin(), excess.begin() + to_copy, out + out_size);
      out_size += to_copy;
    }

    while (out_size < count) {
      ld_check(out_size > 0);
      out[out_size] = out[rng() % out_size];
      ++out_size;
    }
  } else {
    size_t picked_size = out_size;
    ld_assert(std::is_sorted(out, out + picked_size));
    auto was_picked = [&](size_t x) {
      auto it = std::lower_bound(out, out + picked_size, x);
      return it != out + picked_size && *it == x;
    };

    size_t offset = rng() % size;
    for (size_t step_idx = 0; step_idx < size && out_size < count; ++step_idx) {
      size_t x = (offset + step_idx) % size;
      double weight = distribution.weight(x);
      if (weight <= scaled_epsilon) {
        assert(weight >= -scaled_epsilon);
        continue;
      }
      if (mode == Mode::AT_MOST_ONCE && was_picked(x)) {
        continue;
      }
      out[out_size++] = x;
    }

    if (out_size < count) {
      if (out_size_ptr) {
        std::sort(out, out + out_size);
      }
      return Result::IMPOSSIBLE;
    }
  }

  ld_check(out_size == count);
  std::sort(out, out + out_size);
  // In Mode::WHATEVS the probability of getting here should be "zero",
  // so there's no bias.
  return mode == Mode::WHATEVS ? Result::OK : Result::BIASED;
}

} // namespace Sampling

}} // namespace facebook::logdevice
