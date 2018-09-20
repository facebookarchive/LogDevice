/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <cassert>
#include <functional>
#include <vector>

namespace facebook { namespace logdevice {

/**
 * A template for defining array-based maps from the set of values of
 * an enum class to arbitrary objects. It's currently used to define a
 * map from E:: error codes into their string representations and
 * descriptions, and a map from MessageType:: values into their string
 * names and deserializers. See ErrorStrings.h, MessageDeserializers.h.
 */

template <typename Enum,
          typename Val,
          Enum InvalidEnum = Enum::INVALID,
          int Size = static_cast<int>(Enum::MAX)>
class EnumMap {
 public:
  EnumMap() : map_() {
    map_.fill(invalidValue());
    setValues();
  }

  auto begin() const {
    return map_.cbegin();
  }
  auto end() const {
    return map_.cend();
  }
  auto size() const {
    return map_.size();
  }

  const Val& operator[](int n) const {
    if (n >= 0 && n < Size) {
      return map_[n];
    } else {
      return invalidValue();
    }
  }

  const Val& operator[](Enum n) const {
    return (*this)[static_cast<int>(n)];
  }

  template <class T>
  Enum reverseLookup(const T& search_val) const {
    return reverseLookup<T>(
        search_val, [](const T& a, const Val& b) { return a == b; });
  }

  template <class T>
  Enum reverseLookup(const T& search_val,
                     std::function<bool(const T&, const Val&)> cmp) const {
    if (cmp(search_val, invalidValue())) {
      return InvalidEnum;
    }

    int idx = 0;
    for (auto& val : map_) {
      if (cmp(search_val, val)) {
        return static_cast<Enum>(idx);
      }
      ++idx;
    }
    return invalidEnum();
  }

  std::vector<Enum> allValidKeys() const {
    std::vector<Enum> vec;
    for (int i = 0; i < Size; ++i) {
      if (map_[i] != invalidValue()) {
        vec.push_back((Enum)i);
      }
    }
    return vec;
  }

  // set() is only used during setValues() and in tests.
  // In production maps are initialized at startup and never changed.
  template <typename ValT>
  void set(Enum n, ValT&& val) {
    assert(static_cast<int>(n) < Size);
    map_[static_cast<int>(n)] = std::forward<ValT>(val);
  }

  static constexpr Enum invalidEnum() {
    return InvalidEnum;
  }

  // Must be specialized.
  static const Val& invalidValue();

 private:
  // sets map values. This function gets specialized in each class
  void setValues();

  std::array<Val, Size> map_;
};

}} // namespace facebook::logdevice
