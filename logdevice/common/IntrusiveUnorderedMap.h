/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <memory>

#include <boost/intrusive/options.hpp>
#include <boost/intrusive/unordered_set.hpp>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 *  An intrusive hash map that can store items and look up an item with a
 *  given key. Implemented as a wrapper around boost::intrusive::unordered_set.
 */

using IntrusiveUnorderedMapHook = boost::intrusive::unordered_set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

template <typename T,
          typename Key,
          typename KeyOfValue,
          typename KeyHasher = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>,
          typename Disposer = std::default_delete<T>>
class IntrusiveUnorderedMap {
 public:
  // Derived functor classes for internal use
  // TODO: use boost::intrusive::key_of_value once our boost version has
  // upgraded to >= 1.59.0
  struct ValueEqual {
    bool operator()(const T& l, const T& r) const {
      return KeyEqual()(KeyOfValue()(l), KeyOfValue()(r));
    }
  };

  struct ValueHasher {
    size_t operator()(const T& value) const {
      return KeyHasher()(KeyOfValue()(value));
    }
  };

  struct KeyEqualValue {
    bool operator()(const Key& k, const T& v) const {
      return KeyEqual()(k, KeyOfValue()(v));
    }
  };

  using UnorderedSet = boost::intrusive::unordered_set<
      T,
      boost::intrusive::equal<ValueEqual>,
      boost::intrusive::hash<ValueHasher>,
      boost::intrusive::constant_time_size<false>>;

  using bucket_type = typename UnorderedSet::bucket_type;
  using bucket_traits = typename UnorderedSet::bucket_traits;

  /**
   * @param n_buckets   the number of buckets to allocate for the
   *                    underlying intrusive::unordered_set.
   */
  explicit IntrusiveUnorderedMap(size_t n_buckets)
      : buckets_(new bucket_type[n_buckets]),
        map_(bucket_traits(buckets_.get(), n_buckets)) {}

  ~IntrusiveUnorderedMap() {
    // dispose of all remaining Appenders.
    map_.clear_and_dispose(Disposer());
  }

  /**
   * Attempts to insert an item into the map.
   *
   * @return    0 on success, -1 if insertion failed. Sets err to:
   *   EXISTS   if another item with the same value (determined by ValueEqual)
   *            is already in the map
   *   INVALID_PARAM  if the same item is already in the map
   */
  int insert(T& item) {
    if (item.is_linked()) {
      err = E::INVALID_PARAM;
      return -1;
    }

    auto res = map_.insert(item);
    if (!res.second) {
      err = E::EXISTS;
      return -1;
    }

    return 0;
  }

  /**
   * Searches for an item with the specified Key type.
   *
   * @return a pointer to item in the map on success, nullptr if no
   *         item is present in the map for that key.
   */
  T* find(const Key& key) {
    auto it = map_.find(key, KeyHasher(), KeyEqualValue());

    if (it == map_.end()) {
      err = E::NOTFOUND;
      return nullptr;
    }

    return &(*it);
  }

  void clear() {
    map_.clear();
  }

  void clearAndDispose() {
    map_.clear_and_dispose(Disposer());
  }

  size_t size() const {
    return map_.size();
  }

  bool empty() const {
    return size() == 0;
  }

  typename UnorderedSet::iterator begin() {
    return map_.begin();
  }

  typename UnorderedSet::iterator end() {
    return map_.end();
  }

 private:
  std::unique_ptr<bucket_type[]> buckets_;
  UnorderedSet map_;
};

}} // namespace facebook::logdevice
