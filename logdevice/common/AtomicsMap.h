/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>

#include <folly/AtomicHashMap.h>
#include <folly/Memory.h>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file An atomic hash map containing std::atomics. Simple wrapper around
 *       folly::AtomicHashMap.
 */
template <typename Key, typename Value, typename HashFcn = std::hash<Key>>
class AtomicsMap {
 public:
  typedef std::atomic<Value> value_atomic;

  explicit AtomicsMap(size_t size_est) : map_(size_est) {}

  /**
   * Returns a pointer to the atomic corresponding to the specified key. If
   * an entry doesn't exist, a new one will be created with the value
   * provided.
   *
   * @param key  lookup key
   * @param val  the initial value
   *
   * @return A pointer to the atomic or nullptr if the map is full and
   *         insertion failed (err will be set to ENOBUFS in that case).
   *
   */
  value_atomic* insertOrGet(const Key& key, Value val = Value()) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      return it->second.get();
    }

    try {
      auto res = map_.insert(key, std::make_unique<value_atomic>(val));
      return res.first->second.get();
    } catch (const folly::AtomicHashMapFullError&) {
      err = E::NOBUFS;
      return nullptr;
    }
  }

  /**
   * Returns a pointer to the atomic corresponding to the specified key. If
   * an entry doesn't exist, nullptr is returned.
   *
   * @param key  lookup key
   */
  value_atomic* get(const Key& key) const {
    auto it = map_.find(key);
    return (it != map_.end() ? it->second.get() : nullptr);
  }

 private:
  folly::AtomicHashMap<Key, std::unique_ptr<value_atomic>, HashFcn> map_;
};

}} // namespace facebook::logdevice
