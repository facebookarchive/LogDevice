/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <vector>

namespace facebook { namespace logdevice { namespace hashing {

// Consistent hash function. Maps a given key into {0, ..., buckets-1} and has
// a property that, as buckets increases, only a fraction of keys will be
// remapped.
uint64_t ch(uint64_t key, uint64_t buckets);

// A weighted consistent hash function (number of keys mapping to each bucket
// is protportional to its weight). Weights are expected to be real numbers in
// the [0, 1] range.
// Always returns a nonzero-weight bucket, unless all weights are zero.
// @param key        the key to hash,
// @param buckets    number of buckets to hash to,
// @param weight_fn  callback to get weight of a bucket,
// @return  the selected bucket, in [0, buckets).
uint64_t weighted_ch(uint64_t key,
                     uint64_t buckets,
                     std::function<double(uint64_t)> weight_fn);

// Takes explicit vector of weights instead of a callback.
inline uint64_t weighted_ch(uint64_t key, const std::vector<double>& weights) {
  return weighted_ch(key, weights.size(), [&weights](uint64_t bucket) {
    return weights[bucket];
  });
}

}}} // namespace facebook::logdevice::hashing
