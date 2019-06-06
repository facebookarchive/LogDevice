/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/hash.h"

#include <algorithm>
#include <limits>

#include <folly/hash/Hash.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

namespace hashing {

uint64_t ch(uint64_t key, uint64_t buckets) {
  // John Lamping, Eric Veach.
  // A Fast, Minimal Memory, Consistent Hash Algorithm.
  // http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
  uint64_t b = 0, j = 0;
  do {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) *
        (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1));
  } while (j < buckets);
  return b;
}

int64_t weighted_ch(uint64_t key,
                    uint64_t N,
                    const std::function<double(uint64_t)>& weight_fn) {
  // Using ch() as a building block, this function returns a value from the set
  // {0, 1, ..., N-1} such that the probability of an arbitrary key
  // mapping to i-th bucket is weight_fn(i) / W (where W is the sum of all
  // weights). The algorithm, based on mcrouter/lib/WeightedCh3HashFunc.cpp,
  // does several iterations in which it computes h = ch({key, iter}, nbuckets),
  // repeating the process if hash64(key, iter) / 2^64 > weight_fn(h), and
  // returning h otherwise.
  // NOTE: since this algorithm may do multiple iterations depending on
  // weight_fn(h), the hash function is not completely consistent: some movement
  // between existing buckets is possible (the probability drops as weight_fn
  // approach 1.0).
  //
  // Considering that in each iteration weight_fn(h)
  // corresponds to the probability of stopping and returning h, and assuming
  // there's no limit on the number of iterations, probability P[i] of an
  // arbitrary key mapping to i-th bucket is:
  //
  //         P[i] = w[i]/N + P[i] * sum_{j=0}^{N-1}{1 - w[j]}/N
  //
  // (The first term corresponds to ch() returning i-th bucket with probability
  // 1/N and then selecting it, while the second equates to doing another
  // iteration of the loop.)  This results in P[i] = w[i]/W, as desired.
  // In practice, we do at most 128 iterations; the probability of not finding
  // a bucket after this many iterations is negligible, provided that the
  // average node weight is reasonably big (e.g. >= 0.15, in which case that
  // probability is smaller than 1e-9).

  const size_t MAX_TRIES = 128;

  for (uint64_t i = 0; i < MAX_TRIES; ++i) {
    uint64_t k = folly::hash::hash_combine(key, i);

    uint64_t bucket = ch(k, N);
    ld_check(bucket < N);
    double w = weight_fn(bucket);
    ld_check(0 <= w && w <= 1.0);

    double p = static_cast<double>(k) / std::numeric_limits<uint64_t>::max();
    if (p < w) {
      ld_check(w > 0);
      return static_cast<int64_t>(bucket);
    }
  }

  // Bucket was not found even after MAX_TRIES iterations. Pick the first one
  // with non-zero weight.
  for (uint64_t bucket = 0; bucket < N; ++bucket) {
    if (weight_fn(bucket) > 0) {
      return static_cast<int64_t>(bucket);
    }
  }

  // All weights are zero.
  return -1;
}

} // namespace hashing

uint64_t hash_tuple(std::initializer_list<uint64_t> il) {
  uint64_t res = 12011301104159517958ul;
  for (auto it = il.begin(); it != il.end(); ++it) {
    res = folly::hash::hash_128_to_64(res, *it);
  }
  return res;
}

}} // namespace facebook::logdevice
