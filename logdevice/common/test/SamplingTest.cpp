/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sampling.h"

#include <sstream>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::Sampling;

namespace {

class SamplingTest : public ::testing::Test {
 public:
  SamplingTest() {
    rng.seed(5982775905867530240ull, 9936607027721666560ull);
  }

  double
  testDistribution(std::vector<double> weights,
                   size_t sample_size,
                   Mode mode,
                   std::vector<double> fake_weights = std::vector<double>(),
                   bool no_check = false,
                   bool biased = false);

  // Deterministically seeded RNG to make probabilistic tests non-flaky.
  XorShift128PRNG rng;
};

struct ReferenceDistribution {
  std::vector<double> initial_weights;
  std::vector<double> added_weights;
  std::set<size_t> modified_idxs;

  explicit ReferenceDistribution(size_t n)
      : initial_weights(n), added_weights(n) {}

  size_t numUpdatedWeights() const {
    return modified_idxs.size();
  }

  void addWeight(size_t idx, double delta) {
    added_weights[idx] += delta;
    modified_idxs.insert(idx);
  }

  void revert(size_t idx) {
    added_weights[idx] = 0;
    modified_idxs.erase(idx);
  }

  double prefixSum(size_t count) const {
    return std::accumulate(
               initial_weights.begin(), initial_weights.begin() + count, 0.) +
        std::accumulate(
               added_weights.begin(), added_weights.begin() + count, 0.);
  }

  double weight(size_t idx) const {
    return initial_weights[idx] + added_weights[idx];
  }

  size_t findPrefixBySum(double s) {
    double x = 0;
    for (size_t i = 0; i < initial_weights.size(); ++i) {
      x += weight(i);
      if (x > s) {
        return i;
      }
    }
    return initial_weights.size();
  }
};

TEST_F(SamplingTest, AdjustedDistributionTest) {
  ProbabilityDistribution base;
  for (size_t n = 3; n <= 30; ++n) {
    ReferenceDistribution reference(n);
    for (size_t i = 0; i < n; ++i) {
      reference.initial_weights[i] =
          rng() % 3 ? folly::Random::randDouble01(rng) : 0.;
    }

    base.assign(
        reference.initial_weights.begin(), reference.initial_weights.end());
    EXPECT_EQ(n, base.size());
    ProbabilityDistributionAdjustment diff;
    AdjustedProbabilityDistribution d(&base, &diff);

    for (size_t q = 0; q < 500; ++q) {
      const int num_ops = 6;
      int op = rng() % num_ops;

      if (op == 0) {
        size_t count = rng() % (n + 1);
        double expected = reference.prefixSum(count);
        double found = d.prefixSum(count);
        ASSERT_DOUBLE_EQ(expected, found);
      } else if (op == 1) {
        size_t idx = rng() % n;
        double expected = reference.weight(idx);
        double found = d.weight(idx);
        ASSERT_NEAR(expected, found, 1e-13);
      } else if (op == 2) {
        double total = reference.prefixSum(n);
        double s = total * folly::Random::randDouble01(rng);
        if (rng() % 3 == 0) {
          s = rng() % 2 ? 0. : total;
        }

        // Just check that reference implementation works.
        size_t expected = reference.findPrefixBySum(s);
        ld_check(reference.prefixSum(expected) <= s + 1e-13);
        ld_check(reference.prefixSum(std::min(n, expected + 1)) > s - 1e-13);

        // Can't expect `expected` and `found` to be exactly equal because of
        // rounding errors. Instead check that the promise of findPrefixBySum()
        // is held.
        size_t found = d.findPrefixBySum(s);
        ASSERT_LE(reference.prefixSum(found), s + 1e-13);
        ASSERT_GT(reference.prefixSum(std::min(n, found + 1)), s - 1e-13);
      } else if (op == 3) {
        ASSERT_EQ(reference.numUpdatedWeights(), diff.numUpdatedWeights());
      } else if (op == 4) {
        size_t idx = rng() % n;
        double w = reference.weight(idx);
        double delta;
        if (rng() % 2) {
          delta = -w;
        } else {
          delta = folly::Random::randDouble01(rng) * w * 2 - w;
        }
        reference.addWeight(idx, delta);
        diff.addWeight(idx, delta);
      } else if (op == 5) {
        if (!reference.modified_idxs.empty()) {
          size_t offset = rng() % reference.modified_idxs.size();
          auto it = reference.modified_idxs.begin();
          std::advance(it, offset);
          size_t idx = *it;
          reference.revert(idx);
          diff.revert(idx);
        }
      } else {
        static_assert(num_ops == 6, "Don't forget to update num_ops.");
        ld_check(false);
      }
    }
  }
}

TEST_F(SamplingTest, Correctness) {
  ProbabilityDistribution d;

  // Can't sample from empty distribution...
  {
    size_t x;
    EXPECT_EQ(Result::IMPOSSIBLE, sampleOne(d, &x));
  }
  {
    std::vector<int> v(4);
    EXPECT_EQ(Result::IMPOSSIBLE, sampleMulti(d, 4, Mode::WHATEVS, &v[0]));
  }
  // ... except for samples of size 0.
  {
    std::vector<int> v(1);
    EXPECT_EQ(Result::OK, sampleMulti(d, 0, Mode::WHATEVS, &v[0]));
  }

  d.assign({20, 13, 0, 17});

  // Sample one.
  {
    size_t x = 42;
    ASSERT_EQ(Result::OK, sampleOne(d, &x));
    EXPECT_LT(x, 4);
    EXPECT_NE(2, x);
  }

  // Some OK sampleMulti().
  {
    std::vector<int> v(4);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::WHATEVS, &v[0]));
    for (int& y : v) {
      EXPECT_LT(y, 4);
      EXPECT_NE(2, y);
    }
  }
  {
    std::vector<int> v(1);
    EXPECT_EQ(Result::OK, sampleMulti(d, 0, Mode::WHATEVS, &v[0]));
  }
  {
    std::vector<int> v(5);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::AT_LEAST_ONCE, &v[0]));
    size_t c0 = 0, c1 = 0, c3 = 0;
    for (int& y : v) {
      EXPECT_LT(y, 4);
      EXPECT_NE(4, y);
      ++(y ? y == 1 ? c1 : c3 : c0);
    }
    EXPECT_NE(c0, 0);
    EXPECT_NE(c1, 0);
    EXPECT_NE(c3, 0);
  }
  {
    std::vector<int> v(2);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::AT_MOST_ONCE, &v[0]));
    for (int& y : v) {
      EXPECT_LT(y, 4);
      EXPECT_NE(4, y);
    }
    EXPECT_NE(v[0], v[1]);
  }

  // Some IMPOSSIBLE sampleMulti().
  {
    std::vector<int> v(4);
    EXPECT_EQ(Result::IMPOSSIBLE,
              sampleMulti(d, v.size(), Mode::AT_MOST_ONCE, &v[0]));
  }
  {
    std::vector<int> v(2);
    EXPECT_EQ(Result::IMPOSSIBLE,
              sampleMulti(d, v.size(), Mode::AT_LEAST_ONCE, &v[0]));
  }

  // Some BIASED sampleMulti().
  {
    // Impossible for systematic sampling to pick both of the weight 5 items.
    ProbabilityDistribution d2({0, 30, 0, 5, 0, 5, 0, 20, 0});
    std::vector<int> v(4);
    ASSERT_EQ(
        Result::BIASED, sampleMulti(d2, v.size(), Mode::AT_LEAST_ONCE, &v[0]));
    EXPECT_EQ(std::vector<int>({1, 3, 5, 7}), v);
  }
  {
    ProbabilityDistribution d2({0, 35, 0, 5, 0, 5, 0});
    std::vector<int> v(3);
    ASSERT_EQ(
        Result::BIASED, sampleMulti(d2, v.size(), Mode::AT_MOST_ONCE, &v[0]));
    EXPECT_EQ(std::vector<int>({1, 3, 5}), v);
  }

  d.assign({13, 0});

  {
    size_t x = 42;
    ASSERT_EQ(Result::OK, sampleOne(d, &x));
    EXPECT_EQ(0, x);
  }
  {
    std::vector<int> v(3);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::WHATEVS, &v[0]));
    EXPECT_EQ(std::vector<int>({0, 0, 0}), v);
  }
  {
    std::vector<int> v(1);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::AT_LEAST_ONCE, &v[0]));
    EXPECT_EQ(std::vector<int>({0}), v);
  }
  {
    std::vector<int> v(1);
    ASSERT_EQ(Result::OK, sampleMulti(d, v.size(), Mode::AT_MOST_ONCE, &v[0]));
    EXPECT_EQ(std::vector<int>({0}), v);
  }
  {
    std::vector<int> v(2);
    ASSERT_EQ(Result::IMPOSSIBLE,
              sampleMulti(d, v.size(), Mode::AT_MOST_ONCE, &v[0]));
  }

  // All zeros.
  d.assign({0});
  {
    size_t x;
    EXPECT_EQ(Result::IMPOSSIBLE, sampleOne(d, &x));
  }
  {
    std::vector<int> v(1);
    ASSERT_EQ(
        Result::IMPOSSIBLE, sampleMulti(d, v.size(), Mode::WHATEVS, &v[0]));
  }
}

// sample_size = 0  means sampleOne().
// If fake_weights is not empty, it's a "null" test: sample from `fake_weights`,
// but check against real `weights`; expect the check to fail.
double SamplingTest::testDistribution(std::vector<double> weights,
                                      size_t sample_size,
                                      Mode mode,
                                      std::vector<double> fake_weights,
                                      bool no_check,
                                      bool biased) {
  // To test that the distribution is correct, call sample*() many times,
  // count how many times each item was picked, then check that these counts
  // are close enough to what we whould expect
  // (weight / total_weight * sample_size * sample_count).
  // How do we define "close enough"? We could e.g. check that the average
  // difference is below threshold. How do we pick the threshold? We could do
  // an experiment to see how small the difference gets, and set a threshold
  // around that value. But that would need to be done for each set of
  // parameters separately.
  // Instead, this code is trying to be fancy and do an actual statistical test
  // (chi-squared test), which gives a reliable threshold.
  // But the number of iterations is still picked empirically.

  bool single = sample_size == 0;
  sample_size = std::max(1ul, sample_size);
  size_t nonzero_weights = 0;
  double min_nonzero = 1e200;
  double sum_weights = 0;
  for (double x : weights) {
    if (x > 0) {
      ++nonzero_weights;
      min_nonzero = std::min(min_nonzero, x);
      sum_weights += x;
    }
  }
  ld_check(sum_weights > 0);
  ld_check(nonzero_weights > 0);
  ld_check(min_nonzero < 1e100);

  // Approximate 0.999 quantile of chi-squared distribution with
  // nonzero_weights degrees of freedom.
  // So this test will randomly fail in 0.1% of runs.
  const double threshold = 9 * sqrt(nonzero_weights + .0);
  // Number of samples to draw. The fewer the more likely false negatives
  // (test passing when the distribution is actually bad).
  // This is an empirical formula picked using DISABLED_PrintDeviations.
  const size_t iters = 30000 * nonzero_weights /
      sqrt(min_nonzero / sum_weights * nonzero_weights);

  bool fake = !fake_weights.empty();
  if (!fake) {
    fake_weights = weights;
  }
  ProbabilityDistribution d(fake_weights.begin(), fake_weights.end());
  std::vector<size_t> v(sample_size);
  std::vector<size_t> count(weights.size());
  size_t count_biased = 0;
  for (size_t it = 0; it < iters; ++it) {
    Result rv;
    size_t achieved_size = std::numeric_limits<size_t>::max();
    size_t over_or_underweight = std::numeric_limits<size_t>::max();
    if (single) {
      rv = sampleOne(d, &v[0], rng);
    } else {
      rv = sampleMulti(d,
                       sample_size,
                       mode,
                       &v[0],
                       rng,
                       &achieved_size,
                       &over_or_underweight);
    }
    auto process_sample = [&] {
      if (rv == Result::IMPOSSIBLE) {
        return false;
      }
      if (rv == Result::BIASED) {
        EXPECT_LT(over_or_underweight, weights.size());
        if (!biased) {
          return false;
        }

        if (mode == Sampling::Mode::AT_MOST_ONCE) {
          // Should be overweight.
          EXPECT_GT(weights[over_or_underweight],
                    sum_weights / sample_size - Sampling::EPSILON);
        } else {
          ld_check(mode == Sampling::Mode::AT_LEAST_ONCE);
          // Should be underweight.
          EXPECT_LT(weights[over_or_underweight],
                    sum_weights / sample_size + Sampling::EPSILON);
        }

        // Note that if sampling is biased, BIASED doesn't have to be
        // returned every time, only with some probability.
        ++count_biased;
      }
      for (size_t i = 0; i < sample_size; ++i) {
        if (v[i] >= weights.size()) {
          return false;
        }
        if (i &&
            (v[i] < v[i - 1] ||
             (mode == Mode::AT_MOST_ONCE && v[i] == v[i - 1]))) {
          return false;
        }
        if (mode == Mode::AT_LEAST_ONCE) {
          for (size_t j = (i ? v[i - 1] + 1 : 0); j < v[i]; ++j) {
            if (weights[j] != 0) {
              return false;
            }
          }
        }
      }
      if (mode == Mode::AT_LEAST_ONCE) {
        for (size_t j = v.back() + 1; j < weights.size(); ++j) {
          if (weights[j] != 0) {
            return false;
          }
        }
      }
      for (size_t x : v) {
        ++count[x];
      }
      return true;
    };

    if (!process_sample()) {
      std::stringstream ss;
      for (size_t i = 0; i < v.size(); ++i) {
        if (i) {
          ss << ',';
        }
        ss << v[i];
      }
      ld_critical("bad sample: %s", ss.str().c_str());
      ADD_FAILURE();
      return -1;
    }
  }

  double deviation = 0;
  for (size_t i = 0; i < count.size(); ++i) {
    // Expected number of times item i is picked in each sample.
    double ex = weights[i] / sum_weights * sample_size;
    // count[i] comes from a binomial distribution: on each iteration
    // with probability p item i is picked c+1 times, with probability 1-p
    // it's picked c times; this assumes that ProbabilityDistribution does
    // systematic sampling; if it does something else, this may not be true.
    // Approximate it with normal distribution. Also assume that counts are
    // independent, which is untrue but hopefully close enough.
    size_t c = (size_t)floor(ex + 1e-13);
    double p = ex - c;
    if (p < 1e-12) {
      // No randomness. Avoid dividing by zero variance.
      ld_check(p > -1e-12);
      EXPECT_EQ(iters * c, count[i]);
      continue;
    }
    double var = p * (1 - p);
    // This is some measure of how far the observed distribution is from the
    // expected one. This particular measure has the nice property that
    // we know its approximate probability distribution (standard normal),
    // which allows us to set threshold analytically instead of experimentally.
    deviation += pow(count[i] - ex * iters, 2) / (var * iters);
  }
  ld_check(deviation >= 0);

  if (!no_check) {
    if (fake || biased) {
      EXPECT_GT(deviation, threshold);
    } else {
      EXPECT_LT(deviation, threshold);
    }
    if (biased) {
      EXPECT_GT(count_biased, 0);
    }
  }

  return deviation / threshold;
}

// Not a real test, just an app to help with manual tweaking of thresholds
// and number of iterations. Run with --gtest_also_run_disabled_tests.
// Adjust number of iterations so that second number is mostly above 1
// (below 1 is a false negative, i.e. the test would fail to detect a bad
// distribution). That would ensure that tests can detect a 5% error
// in distribution.
// Second number depends on `iters` approximately linearly, and depends on
// weights and sample_size in some unknown way.
// First number should be consistently below 1 (corresponds to false positives,
// i.e. test flakiness), but it almost doesn't depend on `iters`, only on
// `threshold`, which is unlikely to need tweaking because it's relatively sound
// statistically.
TEST_F(SamplingTest, DISABLED_PrintDeviations) {
  auto test = [&](std::string name,
                  std::vector<double> weights,
                  size_t /*sample_size*/) {
    auto perturbed = weights;
    perturbed[0] *= 1.05;
    // These first number should be less than one, the second should be greater
    // than one. If too much less or too much greater, we're probably
    // wasting iterations.
    std::cerr << name
              << testDistribution(weights, 1, Mode::WHATEVS, weights, true)
              << " "
              << testDistribution(perturbed, 1, Mode::WHATEVS, weights, true)
              << std::endl;
  };

  // Uniform distribution with different number of items and sample size.
  test("uniform 2, pick 1: ", {1, 1}, 1);
  test("uniform 2, pick 2: ", {1, 1}, 2);
  test("uniform 3, pick 1: ", {1, 1, 1}, 1);
  test("uniform 4, pick 1: ", std::vector<double>(4, 1), 1);
  test("uniform 4, pick 2: ", std::vector<double>(4, 1), 2);
  test("uniform 7, pick 1: ", std::vector<double>(7, 1), 1);
  test("uniform 7, pick 2: ", std::vector<double>(7, 1), 2);
  test("uniform 7, pick 3: ", std::vector<double>(7, 1), 3);
  test("uniform 15, pick 1: ", std::vector<double>(15, 1), 1);
  test("uniform 15, pick 2: ", std::vector<double>(15, 1), 2);
  test("uniform 15, pick 3: ", std::vector<double>(15, 1), 3);
  test("uniform 15, pick 5: ", std::vector<double>(15, 1), 5);
  test("uniform 15, pick 7: ", std::vector<double>(15, 1), 7);

  // Different weights.
  test("1/15 10x smaller, pick 1: ",
       {0.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("1/15 10x smaller, pick 2: ",
       {0.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       2);
  test("1/15 10x smaller, pick 3: ",
       {0.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       3);
  test("1/15 10x smaller, pick 5: ",
       {0.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       5);
  test("1/15 10x smaller, pick 7: ",
       {0.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       7);

  test("1/15 10x bigger, pick 1: ",
       {1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("1/15 10x bigger, pick 2: ",
       {1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       2);
  test("1/15 10x bigger, pick 3: ",
       {1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       3);
  test("1/15 10x bigger, pick 5: ",
       {1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       5);
  test("1/15 10x bigger, pick 7: ",
       {1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
       7);

  test("7/15 2x smaller, pick 1: ",
       {.5, .5, .5, .5, .5, .5, .5, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("7/15 2x smaller, pick 2: ",
       {.5, .5, .5, .5, .5, .5, .5, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("7/15 2x smaller, pick 3: ",
       {.5, .5, .5, .5, .5, .5, .5, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("7/15 2x smaller, pick 5: ",
       {.5, .5, .5, .5, .5, .5, .5, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
  test("7/15 2x smaller, pick 7: ",
       {.5, .5, .5, .5, .5, .5, .5, 1, 1, 1, 1, 1, 1, 1, 1},
       1);
}

// Check that distribution test can detect a 15% error.
TEST_F(SamplingTest, NullTest) {
  testDistribution({1, 1.15, 1, 1, 1}, 1, Mode::WHATEVS, {1, 1, 1, 1, 1});
  testDistribution({.5, .5, .5, .5, 0.85, 1, 1, 1},
                   3,
                   Mode::WHATEVS,
                   {.5, .5, .5, .5, 1, 1, 1, 1});
}

TEST_F(SamplingTest, DistributionUniform) {
  for (std::vector<double> w :
       {std::vector<double>({1, 1, 1, 1, 1}),
        std::vector<double>({0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0})}) {
    for (size_t k = 0; k <= 5; ++k) {
      testDistribution(w, k, Mode::AT_MOST_ONCE);
      testDistribution(w, k + 5, Mode::AT_LEAST_ONCE);
    }
  }
}

TEST_F(SamplingTest, DistributionRandom) {
  for (size_t n : {3, 7, 10}) {
    std::vector<double> v(n);
    for (double& w : v) {
      w = folly::Random::randDouble01(rng) + 1;
    }
    for (int add_zeros = 0;; ++add_zeros) {
      testDistribution(v, std::min(3ul, n / 2 + 1), Mode::AT_MOST_ONCE);
      testDistribution(v, std::max(15ul, 2 * n - 1), Mode::AT_LEAST_ONCE);

      if (add_zeros == 1) {
        break;
      }
      // Intersperse weight vector with zeros.
      std::vector<double> new_v(n * 2 + 1);
      for (size_t i = 0; i < n; ++i) {
        new_v[i * 2 + 1] = v[i];
      }
      v = new_v;
    }
  }
}

TEST_F(SamplingTest, Biased) {
  testDistribution({1, 1.2}, 2, Mode::AT_MOST_ONCE, {}, false, true);
  testDistribution({1, 1.2}, 2, Mode::AT_LEAST_ONCE, {}, false, true);

  testDistribution({1, 1, 1, 1, 1, 3}, 3, Mode::AT_MOST_ONCE, {}, false, true);
  testDistribution({1, 1, 3}, 4, Mode::AT_LEAST_ONCE, {}, false, true);
}

} // anonymous namespace
