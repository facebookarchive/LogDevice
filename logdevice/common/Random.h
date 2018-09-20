/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>

#include <folly/Random.h>
#include <folly/ThreadLocal.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * @file  Interface for different random number generators and wrappers around
 *        a couple of implementations.
 */

/**
 * an abstraction that returns pseudo-random numbers
 */
class RNG {
 public:
  using result_type = uint32_t;

  // returns a random number
  virtual uint32_t operator()() = 0;
  virtual ~RNG() {}

  static constexpr uint32_t min() {
    return std::numeric_limits<uint32_t>::min();
  }
  static constexpr uint32_t max() {
    return std::numeric_limits<uint32_t>::max();
  }
};

/**
 * A thread-safe RNG.
 */
class DefaultRNG : public RNG {
 public:
  DefaultRNG() = default;

  uint32_t operator()() override {
    return folly::Random::rand32();
  }

  static_assert(folly::ThreadLocalPRNG::min() == RNG::min() &&
                    folly::ThreadLocalPRNG::max() == RNG::max(),
                "unexpected RNG range");

  // Returns a singleton instance.
  static DefaultRNG& get();
};

/**
 * A Xorshift128 implementation from wiki.  A good choice for deterministic
 * shuffling based on a hash; its small state can make it much faster to
 * initialise than typical choices like MT19937.
 *
 * Not thread-safe.
 *
 * NOTE: Changing this implementation may be risky in terms of push safety
 * because we may depend on servers deterministically producing the same
 * output for the same seed.
 */
class XorShift128PRNG : public RNG {
 public:
  // Construct unseeded RNG. You must call seed() before using operator().
  XorShift128PRNG() = default;

  // Construct a seeded RNG.
  XorShift128PRNG(uint64_t h1, uint64_t h2) {
    seed(h1, h2);
  }

  void seed(uint32_t value[4]) {
    x = value[0];
    y = value[1];
    z = value[2];
    w = value[3];
  }
  void seed(uint64_t h1, uint64_t h2) {
    x = (uint32_t)h1;
    y = (uint32_t)(h1 >> 32);
    z = (uint32_t)h2;
    w = (uint32_t)(h2 >> 32);
  }
  uint32_t operator()() override {
    // make sure the RNG was seeded
    ld_check((x | y | z | w) != 0);

    // xorshift128 implementation from the wiki
    uint32_t t = x;
    t ^= t << 11;
    t ^= t >> 8;
    x = y;
    y = z;
    z = w;
    w ^= w >> 19;
    w ^= t;
    return w;
  }
  static_assert(std::numeric_limits<uint32_t>::min() == RNG::min() &&
                    std::numeric_limits<uint32_t>::max() == RNG::max(),
                "unexpected RNG range");

 private:
  // xorshift's state
  uint32_t x{0};
  uint32_t y{0};
  uint32_t z{0};
  uint32_t w{0};
};

/**
 * An alternative for std::shuffle().  Why?
 *
 * 1) Push safety.  std::shuffle() may change when the compiler is upgraded,
 *    which introduces issues because we may depend on servers
 *    deterministically producing the same output with the same RNG.
 *
 * 2) Performance.  std::shuffle() understandably uses
 *    std::uniform_int_distribution to get random numbers in a range, however
 *    that generates a lot of code to eliminate bias.
 *
 *    For shuffling small vectors, the bias introduced by modding is negligible.
 *    This implementation can be used for smaller and faster shuffling.
 */
template <typename RandomIter, typename RNG>
void simple_shuffle(RandomIter begin, RandomIter end, RNG&& rng) {
  int n = end - begin;
  for (int i = 0; i < n - 1; ++i) {
    int j = i + rng() % (n - i);
    std::swap(*(begin + i), *(begin + j));
  }
}

}} // namespace facebook::logdevice
