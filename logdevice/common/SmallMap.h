/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <type_traits>
#include <vector>

#include <folly/small_vector.h>

namespace facebook { namespace logdevice {

// Associative container optimized for very small number of keys
// (up to 10, maybe 100). Internally it's just a vector of key-value pairs.
//
// Comes in two flavors:
//  * Ordered. The internal vector is kept sorted.
//    Insert is O(n), search is O(log n) time.
//    (More precisely, insert(x) takes O(number of keys greater than x) time.)
//    Iterator iterates in order of increasing key.
//    Supports lower_bound() and upper_bound().
//  * Unordered. The internal vector is not sorted.
//    Insert is O(1), search is O(n) time.
//    Iterator iterates in unspecified order. (Ok, it's insertion order, but we
//    reserve the right to change implementation to produce some other order.)
//
// So, the asymptotic time is pretty bad but for very small n it should be
// faster std::unordered_map, and there are no memory alocations if the
// underlying container is folly::small_vector.
//
// Due to implementation shortcuts, there are subtleties in iterators:
//  * value_type is pair<Key, T> instead of pair<const Key, T>.
//    Please don't modify the keys.
//  * Iterators support random access. It may be a good idea to avoid relying
//    on it, in case someone wants to change the implementation in future
//    (to something with better asymptotics, e.g. automatically
//    switching from vector to a hash map when the number of keys grows big).

template <typename Key, typename T, typename ContainerType, bool Sorted>
class BasicSmallMap {
 public:
  using key_type = Key;
  using mapped_type = T;
  using value_type = std::pair<Key, T>;
  using iterator = typename ContainerType::iterator;
  using const_iterator = typename ContainerType::const_iterator;

  static_assert(
      std::is_same<value_type, typename ContainerType::value_type>::value,
      "ContainerType must have value_type of std::pair<Key, T>");

  struct LowerBoundCompare {
    bool operator()(const value_type& a, const Key& b) {
      return a.first < b;
    }
  };
  struct UpperBoundCompare {
    bool operator()(const Key& a, const value_type& b) {
      return a < b.first;
    }
  };

  BasicSmallMap() = default;

  size_t size() const {
    return vector_.size();
  }
  bool empty() const {
    return vector_.empty();
  }
  void clear() {
    vector_.clear();
  }
  iterator erase(const_iterator pos) {
    return vector_.erase(pos);
  }
  size_t erase(const Key& key) {
    auto it = find(key);
    if (it == end()) {
      return 0;
    }
    erase(it);
    return 1;
  }

  iterator begin() {
    return vector_.begin();
  }
  iterator end() {
    return vector_.end();
  }
  const_iterator begin() const {
    return vector_.begin();
  }
  const_iterator end() const {
    return vector_.end();
  }
  const_iterator cbegin() const {
    return vector_.cbegin();
  }
  const_iterator cend() const {
    return vector_.cend();
  }

  // {lower,upper}_bound() are enabled only if Sorted = true.
  template <bool Sorted2 = Sorted>
  typename std::enable_if<Sorted2, iterator>::type lower_bound(const Key& k) {
    static_assert(
        Sorted2 == Sorted, "Don't pass template arguments to lower_bound().");
    return std::lower_bound(begin(), end(), k, LowerBoundCompare());
  }
  template <bool Sorted2 = Sorted>
  typename std::enable_if<Sorted2, const_iterator>::type
  lower_bound(const Key& k) const {
    static_assert(
        Sorted2 == Sorted, "Don't pass template arguments to lower_bound().");
    return std::lower_bound(begin(), end(), k, LowerBoundCompare());
  }
  template <bool Sorted2 = Sorted>
  typename std::enable_if<Sorted2, iterator>::type upper_bound(const Key& k) {
    static_assert(
        Sorted2 == Sorted, "Don't pass template arguments to upper_bound().");
    return std::upper_bound(begin(), end(), k, UpperBoundCompare());
  }
  template <bool Sorted2 = Sorted>
  typename std::enable_if<Sorted2, const_iterator>::type
  upper_bound(const Key& k) const {
    static_assert(
        Sorted2 == Sorted, "Don't pass template arguments to upper_bound().");
    return std::upper_bound(begin(), end(), k, UpperBoundCompare());
  }

  iterator find(const Key& k) {
    if (Sorted) {
      iterator it = std::lower_bound(begin(), end(), k, LowerBoundCompare());
      if (it != end() && it->first == k) {
        return it;
      }
      return end();
    } else {
      return std::find_if(
          begin(), end(), [&k](const value_type& v) { return v.first == k; });
    }
  }

  const_iterator find(const Key& k) const {
    if (Sorted) {
      const_iterator it =
          std::lower_bound(begin(), end(), k, LowerBoundCompare());
      if (it != end() && it->first == k) {
        return it;
      }
      return end();
    } else {
      return std::find_if(
          begin(), end(), [&k](const value_type& v) { return v.first == k; });
    }
  }

  size_t count(const Key& k) const {
    return find(k) != end();
  }

  std::pair<iterator, bool> insert(value_type&& v) {
    if (Sorted) {
      // Find the place for new key in the sorted vector.
      // Do it from right to left to do inserts to the end in O(1) time.
      for (int i = (int)vector_.size() - 1; i >= 0; --i) {
        if (vector_[i].first <= v.first) {
          if (vector_[i].first == v.first) {
            return std::make_pair(begin() + i, false);
          }
          return std::make_pair(
              vector_.insert(vector_.begin() + i + 1, std::move(v)), true);
        }
      }
      return std::make_pair(
          vector_.insert(vector_.begin(), std::move(v)), true);
    } else {
      iterator it = find(v.first);
      if (it != end()) {
        return std::make_pair(it, false);
      }
      vector_.push_back(std::move(v));
      return std::make_pair(end() - 1, true);
    }
  }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  T& operator[](const Key& k) {
    // Could implement shorter by calling this->emplace(k, T()), but that would
    // construct T unnecessarily when `k` is already in the map.

    if (Sorted) {
      iterator it = std::lower_bound(begin(), end(), k, LowerBoundCompare());
      if (it != end() && it->first == k) {
        return it->second;
      }
      return vector_.emplace(it, k, T())->second;
    } else {
      iterator it = find(k);
      if (it != end()) {
        return it->second;
      }
      vector_.emplace_back(k, T());
      return vector_.back().second;
    }
  }
  T& at(const Key& k) {
    iterator it = find(k);
    if (it == end()) {
      throw std::out_of_range("BasicSmallMap::at(): key not in the map");
    }
    return it->second;
  }
  const T& at(const Key& k) const {
    const_iterator it = find(k);
    if (it == end()) {
      throw std::out_of_range("BasicSmallMap::at(): key not in the map");
    }
    return it->second;
  }

 private:
  ContainerType vector_;
};

template <typename Key, typename T, size_t InlineSize = 4>
using SmallUnorderedMap =
    BasicSmallMap<Key,
                  T,
                  folly::small_vector<std::pair<Key, T>, InlineSize>,
                  false>;

template <typename Key, typename T, size_t InlineSize = 4>
using SmallOrderedMap =
    BasicSmallMap<Key,
                  T,
                  folly::small_vector<std::pair<Key, T>, InlineSize>,
                  true>;

// "Recursive" ones allow T to be incomplete type. In particular, this pattern:
//   struct A {
//     SmallRecursiveUnorderedMap<int, A> children;
//   };
// wouldn't work with non-"recursive" SmallUnorderedMap because the
// underlying folly::small_vector<pair<int, A>> would store at least one
// instance of A inline, so sizeof(A::children) would be greater than
// sizeof(A), which is impossible.
template <typename Key, typename T>
using SmallRecursiveUnorderedMap =
    BasicSmallMap<Key, T, std::vector<std::pair<Key, T>>, false>;
template <typename Key, typename T>
using SmallRecursiveOrderedMap =
    BasicSmallMap<Key, T, std::vector<std::pair<Key, T>>, false>;

}} // namespace facebook::logdevice
