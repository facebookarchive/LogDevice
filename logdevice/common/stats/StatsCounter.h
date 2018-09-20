/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstdint>

namespace facebook { namespace logdevice {

using stats_counter_raw_t = int64_t;

/**
 * A simple wrapper around std::atomic that performs all operations (at least
 * those we care about) with std::memory_order_relaxed instead of the default
 * std::memory_order_seq_cst. This is good enough for eventual consistency of
 * stat data.
 */
class StatsCounter : private std::atomic<stats_counter_raw_t> {
 public:
  using raw_t = stats_counter_raw_t;

 private:
  using Base = std::atomic<raw_t>;
  static constexpr std::memory_order MEM_ORDER = std::memory_order_relaxed;

 public:
  /* implicit */ StatsCounter(raw_t initial = 0) noexcept : Base(initial) {}
  StatsCounter(const StatsCounter& other) noexcept : Base(other.load()) {}
  StatsCounter& operator=(raw_t desired) noexcept {
    store(desired);
    return *this;
  }
  StatsCounter& operator=(const StatsCounter& other) noexcept {
    return (*this) = other.load();
  }
  /* implicit */ operator raw_t() const noexcept {
    return load();
  }
  explicit operator bool() const noexcept {
    return !!load();
  }
  raw_t operator++() noexcept {
    return (*this) += 1;
  }
  raw_t operator++(int) noexcept {
    return Base::fetch_add(1, MEM_ORDER);
  }
  raw_t operator--() noexcept {
    return (*this) -= 1;
  }
  raw_t operator--(int) noexcept {
    return Base::fetch_sub(1, MEM_ORDER);
  }
  raw_t operator+=(raw_t arg) noexcept {
    return Base::fetch_add(arg, MEM_ORDER) + arg;
  }
  raw_t operator-=(raw_t arg) noexcept {
    return Base::fetch_sub(arg, MEM_ORDER) - arg;
  }
  raw_t operator&=(raw_t arg) noexcept {
    return Base::fetch_and(arg, MEM_ORDER) & arg;
  }
  raw_t operator|=(raw_t arg) noexcept {
    return Base::fetch_or(arg, MEM_ORDER) | arg;
  }
  raw_t operator^=(raw_t arg) noexcept {
    return Base::fetch_xor(arg, MEM_ORDER) ^ arg;
  }
  raw_t load() const noexcept {
    return Base::load(MEM_ORDER);
  }
  void store(raw_t desired) noexcept {
    Base::store(desired, MEM_ORDER);
  }
};

}} // namespace facebook::logdevice
