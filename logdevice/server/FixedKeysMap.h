/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>
#include <folly/FBVector.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * An efficient map implementation for cases when you know the set of keys
 * in advance, and multiple maps can share the set of keys.
 *
 * Usage:
 *
 * // First build list of keys.
 * FixedKeysMapKeys<std::string> keys;
 * keys.add("foo");
 * keys.add("bar");
 * keys.add("bar");
 * // Call finalize() to deduplicate and make immutable.
 * keys.finalize();
 *
 * // Create a map. `keys` has to outlive `map`.
 * // Multiple maps can share the same keys object.
 * FixedKeysMap<std::string, int> map(&keys);
 * ld_check(map["bar"] == 0);
 * map["bar"] = 42;
 * ld_check(map["bar"] == 42);
 * // the following would fail an assertion:
 * // map["foobar"] = 3;
 *
 * // Can iterate over key-value pairs.
 * for (auto it: map) {
 *   // Unlike standard map, it.second is a pointer to value.
 *   std::cout << "key: " << it.first << ", value: " << *it.second << '\n';
 *   // Can change values while iterating.
 *   *it.second *= 3;
 * }
 */

// K needs to have operator< defined.
template <typename K>
class FixedKeysMapKeys {
 public:
  using iterator = typename std::vector<K>::const_iterator;

  void add(const K& key) {
    ld_check(!final_);
    keys_.push_back(key);
  }
  void finalize() {
    ld_check(!final_);
    std::sort(keys_.begin(), keys_.end());
    keys_.erase(std::unique(keys_.begin(), keys_.end()), keys_.end());
    final_ = true;
  }

  size_t getIdx(const K& key) const {
    ld_check(final_);
    auto it = std::lower_bound(keys_.begin(), keys_.end(), key);
    ld_check(it != keys_.end() && !(key < *it));
    return it - keys_.begin();
  }

  size_t size() const {
    ld_check(final_);
    return keys_.size();
  }
  iterator begin() const {
    ld_check(final_);
    return keys_.cbegin();
  }
  iterator end() const {
    ld_check(final_);
    return keys_.cend();
  }
  const K& operator[](size_t i) const {
    ld_check(i < size());
    return keys_[i];
  }

 private:
  std::vector<K> keys_;
  bool final_ = false;
};

template <typename K, typename V>
class FixedKeysMap {
 public:
  using Keys = FixedKeysMapKeys<K>;
  using value_type = std::pair<const K&, V*>;

  // Iterator dereferences to a std::pair<const K&, V*>
  // (note the unusual pointer in V*). This is because key and value
  // are not stored next to each other, so the customary std::pair<const K, V>&
  // is impossible.
  class iterator : public boost::iterator_facade<iterator,
                                                 value_type,
                                                 boost::forward_traversal_tag,
                                                 value_type> {
   public:
    explicit iterator(typename Keys::iterator k_it,
                      typename folly::fbvector<V>::iterator v_it)
        : k_it_(k_it), v_it_(v_it) {}
    bool equal(const iterator& rhs) const {
      ld_check((k_it_ == rhs.k_it_) == (v_it_ == rhs.v_it_));
      return k_it_ == rhs.k_it_;
    }
    void increment() {
      ++k_it_;
      ++v_it_;
    }
    value_type dereference() const {
      return value_type(*k_it_, &*v_it_);
    }

   private:
    typename Keys::iterator k_it_;
    typename folly::fbvector<V>::iterator v_it_;
  };

  // `keys` has to outlive this `Values`.
  // `keys` has to be finalize()d.
  explicit FixedKeysMap(const Keys* keys)
      : keys_(keys), values_(keys_->size()) {}

  const Keys* keys() const {
    return keys_;
  }

  V& operator[](const K& key) {
    return values_[keys_->getIdx(key)];
  }
  const V& operator[](const K& key) const {
    return values_[keys_->getIdx(key)];
  }
  iterator begin() {
    return iterator(keys_->begin(), values_.begin());
  }
  iterator end() {
    return iterator(keys_->end(), values_.end());
  }

 private:
  const Keys* keys_;
  folly::fbvector<V> values_;
};

}} // namespace facebook::logdevice
