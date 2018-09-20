/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * A simple optional wrapper like folly::Optional but:
 *  1) all operations are atomic with sequential consistency,
 *  2) only works with trivially copyable types,
 *  3) doesn't allow removing value once it's added (it would make 1) harder).
 */

enum optional_detail_empty_tag_t { EMPTY_OPTIONAL };

template <typename T>
class AtomicOptional {
 public:
  AtomicOptional() {}

  explicit AtomicOptional(T val) : value_(val), initialized_(true) {}

  // Allows specifying value for an "uninitialized" instance.
  // This value can be used by fetchMax.
  AtomicOptional(T val, optional_detail_empty_tag_t)
      : value_(val), initialized_(false) {}

  bool hasValue() const {
    return initialized_.load();
  }

  T load() const {
    ld_assert(initialized_.load());
    return value_.load();
  }

  folly::Optional<T> loadOptional() const {
    if (initialized_.load()) {
      return folly::Optional<T>(value_.load());
    } else {
      return folly::Optional<T>();
    }
  }

  void store(T val) {
    value_.store(val);
    initialized_.store(true);
  }

  // Stores max(load(), val). Returns value before update.
  // If !hasValue(), T() or the value provided in constructor is used.
  T fetchMax(T val) {
    T res = atomic_fetch_max(value_, val);
    initialized_.store(true);
    return res;
  }

 private:
  std::atomic<T> value_;
  std::atomic<bool> initialized_{false};
};

}} // namespace facebook::logdevice
