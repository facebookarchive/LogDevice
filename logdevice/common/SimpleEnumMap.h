/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <type_traits>

#include <folly/small_vector.h>

namespace facebook { namespace logdevice {

/**
 * tl;dr: Like EnumMap where you create each entry once and never change it.
 * Values can be large objects.
 *
 * A template for defining array-based maps from the set of values of an enum
 * class to arbitrary objects.
 *
 * Differences with EnumMap:
 *
 * - Values aren't default constructed and don't need a default constructor.
 * - If you never move the map, then your values don't need a move constructor.
 * - We don't need an invalid value or enum.
 * - Don't need to specialize SimpleEnumMap to provide way to fill it.
 * - Can't be sparse.
 * - Can't index by integral type.
 * - No bounds checking when indexing.
 */

template <typename Enum,
          typename Val,
          size_t Size = static_cast<size_t>(Enum::MAX)>
class SimpleEnumMap {
 public:
  static constexpr size_t capacity = Size;

  template <typename Func>
  explicit SimpleEnumMap(const Func& fill) {
    fill();
    ld_check(map_.size() == Size);
  }

  explicit SimpleEnumMap(std::initializer_list<std::pair<const Enum, Val>> init)
      : SimpleEnumMap([&] {
          for (const auto& entry : init) {
            // Key sequence must be in order and contiguous.
            ld_check(static_cast<size_t>(entry.first) == map_.size());

            map_.emplace_back(entry.second);
          }
        }) {}

  const Val& operator[](Enum e) const {
    return map_[static_cast<size_t>(e)];
  }

  Val& operator[](Enum e) {
    return map_[static_cast<size_t>(e)];
  }

  template <class... Args>
  void emplace_back(Args&&... args) {
    map_.emplace_back(std::forward<Args>(args)...);
  }

 private:
  folly::small_vector<Val, Size, folly::small_vector_policy::NoHeap> map_;
};
}} // namespace facebook::logdevice
