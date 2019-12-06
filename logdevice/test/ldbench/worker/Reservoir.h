/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <vector>

#include <folly/Random.h>

namespace facebook { namespace logdevice { namespace ldbench {

/**
 * A simple sampling reservoir that implements Algorithm R by J. Vitter.
 *
 * Sampling allows us to put a cap on memory usage for long-running benchmarks.
 */
template <typename T>
class Reservoir {
 private:
  using Impl = std::vector<T>;

 public:
  using ConstIterator = typename Impl::const_iterator;
  using Iterator = typename Impl::iterator;

  explicit Reservoir(size_t capacity);
  ~Reservoir();

  void put(const T& sample);

  ConstIterator begin() const {
    return impl_.begin();
  }
  ConstIterator end() const {
    return impl_.end();
  }

  Iterator begin() {
    return impl_.begin();
  }
  Iterator end() {
    return impl_.end();
  }

  size_t getCapacity() const noexcept {
    return capacity_;
  }
  size_t getNumberOfInsertions() const noexcept {
    return ninsertions_;
  }
  size_t getNumberOfSamples() const noexcept {
    return impl_.size();
  }

 private:
  size_t capacity_;
  size_t ninsertions_;
  Impl impl_;
  folly::ThreadLocalPRNG prng_;
};

template <typename T>
Reservoir<T>::Reservoir(size_t capacity)
    : capacity_(capacity), ninsertions_(0) {}

template <typename T>
Reservoir<T>::~Reservoir() = default;

template <typename T>
void Reservoir<T>::put(const T& sample) {
  // For details see "Algorithm R" by J. Vitter.
  ++ninsertions_;
  if (ninsertions_ <= capacity_) {
    impl_.emplace_back(sample);
  } else {
    size_t victim_off = prng_() % ninsertions_;
    if (victim_off < capacity_) {
      impl_[victim_off] = sample;
    }
  }
}

}}} // namespace facebook::logdevice::ldbench
