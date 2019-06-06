/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <random>
#include <vector>

#include <gtest/gtest.h>
#include <logdevice/common/hash.h>
#include <logdevice/include/types.h>

#include "logdevice/common/checks.h"

namespace ld = facebook::logdevice;

#define ASSERT_RELATIVE(expected, val, rel_err) \
  ASSERT_LT(fabs(((val) - (expected)) / (expected)), (rel_err))

TEST(HashingTest, Weighted) {
  const size_t nkeys = 2000000;
  std::mt19937_64 rnd(0xbabadeda);

  std::vector<double> w1, w2;
  {
    // 20 buckets with random weights
    std::uniform_real_distribution<double> d(0.0, 1.0);
    for (size_t i = 0; i < 20; ++i) {
      w1.push_back(d(rnd));
    }
    // add another bucket with weight 1.0
    w2 = w1;
    w2.push_back(1.0);
  }

  double W1 = std::accumulate(w1.begin(), w1.end(), 0.0);
  double W2 = std::accumulate(w2.begin(), w2.end(), 0.0);

  std::uniform_int_distribution<uint64_t> d(1, ld::LOGID_MAX.val_);
  std::vector<int> freq1(w1.size(), 0);
  int remapped = 0;

  for (size_t i = 0; i < nkeys; ++i) {
    uint64_t key = d(rnd);

    uint64_t h1 = ld::hashing::weighted_ch(key, w1);
    uint64_t h2 = ld::hashing::weighted_ch(key, w2);
    ASSERT_TRUE(h1 < w1.size());
    ASSERT_TRUE(h2 < w2.size());

    // Keep track of the number of keys mapping to each bucket.
    ++freq1[h1];

    // Since the weight of the new bucket is 1.0, we expects all keys to either
    // stays at the same place or move to the new bucket.
    ASSERT_TRUE(h1 == h2 || h2 == w2.size() - 1);
    remapped += (h1 != h2);
  }

  // verify that the distribution of keys is satisfactory
  for (size_t i = 0; i < w1.size(); ++i) {
    ASSERT_RELATIVE(w1[i] / W1 * nkeys, freq1[i], 0.01);
  }
  // number of keys that were remapped is expected to be proportional to the
  // weight of the newly added bucket
  ASSERT_RELATIVE(*w2.rbegin() / W2 * nkeys, remapped, 0.01);
}

// Tests rehashing by setting weights to zero. Verifies that iterative rehashing
// yields the same results as setting all weights to zero upfront.
TEST(HashingTest, WeightZeroRehashing) {
  const size_t nkeys = 100000;

  std::vector<double> w1(10, 1.0);
  std::vector<double> w2 = w1;
  for (size_t i = 0; i < w2.size() / 2; ++i) {
    w2[i] = 0.0;
  }

  auto ch_iterative = [&](uint64_t key) -> uint64_t {
    std::vector<double> weights = w1;
    for (size_t i = 0; i < w1.size(); ++i) {
      int64_t h = ld::hashing::weighted_ch(key, weights);
      ld_check(h != -1);
      if (w2[h] > 0.0) {
        return h;
      }
      weights[h] = 0;
    }
    ld_check(false);
    return 0;
  };

  std::mt19937_64 rnd(0xbabadeda);
  std::uniform_int_distribution<uint64_t> d(1, ld::LOGID_MAX.val_);

  for (uint64_t i = 0; i < nkeys; ++i) {
    uint64_t key = d(rnd);

    uint64_t h1 = ld::hashing::weighted_ch(key, w2);
    uint64_t h2 = ch_iterative(key);

    EXPECT_EQ(h1, h2);
    EXPECT_LT(0.0, w2[h1]);
  }
}
