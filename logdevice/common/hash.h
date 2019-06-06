/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <vector>

namespace facebook { namespace logdevice {

namespace hashing {

// Consistent hash function. Maps a given key into {0, ..., buckets-1} and has
// a property that, as buckets increases, only a fraction of keys will be
// remapped.
uint64_t ch(uint64_t key, uint64_t buckets);

// A weighted consistent hash function (number of keys mapping to each bucket
// is protportional to its weight). Weights are expected to be real numbers in
// the [0, 1] range.
// On success, returns the selected bucket, in [0, buckets).
// If all weights are zero, returns -1.
// Always returns a nonzero-weight bucket, unless all weights are zero.
// @param key        the key to hash,
// @param buckets    number of buckets to hash to,
// @param weight_fn  callback to get weight of a bucket,
// @return  the selected bucket, in [0, buckets), or -1.
int64_t weighted_ch(uint64_t key,
                    uint64_t buckets,
                    const std::function<double(uint64_t)>& weight_fn);

// Takes explicit vector of weights instead of a callback.
inline int64_t weighted_ch(uint64_t key, const std::vector<double>& weights) {
  return weighted_ch(key, weights.size(), [&weights](uint64_t bucket) {
    return weights[bucket];
  });
}

} // namespace hashing

// Hash a tuple of 64-bit numbers using a chain of folly::hash_128_to_64() calls
// (with special cases for empty and single-element tuples, see code).
// This is different from folly::hash::hash_combine(), which also applies
// std::hash() to each argument, which makes it unnecessarily slower and
// dependent on the implementation of standard library.
uint64_t hash_tuple(std::initializer_list<uint64_t> il);

}} // namespace facebook::logdevice
