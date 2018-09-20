/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>

/**
 * @file Macro for "strong typedef" which creates a new type instead of just
 *       an alias for the original type.  Primarily intended to wrap integer
 *       types.  Comparison operators as well as a simple hash are provided
 *       so the type can be used as a key in standard library containers.
 *
 * Loosely based on boost::strong_typedef.
 * (C) Copyright 2002 Robert Ramey - http://www.rrsd.com .
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 */

#define LOGDEVICE_STRONG_TYPEDEF(RawType, NewType)                    \
  struct NewType {                                                    \
    typedef RawType raw_type;                                         \
    RawType val_;                                                     \
    explicit constexpr NewType(const RawType v) noexcept : val_(v) {} \
    NewType() noexcept : val_() {}                                    \
    explicit operator RawType() const {                               \
      return val_;                                                    \
    }                                                                 \
    constexpr RawType val() const {                                   \
      return val_;                                                    \
    }                                                                 \
    constexpr bool operator==(const NewType& rhs) const {             \
      return val_ == rhs.val_;                                        \
    }                                                                 \
    constexpr bool operator!=(const NewType& rhs) const {             \
      return val_ != rhs.val_;                                        \
    }                                                                 \
    constexpr bool operator<(const NewType& rhs) const {              \
      return val_ < rhs.val_;                                         \
    }                                                                 \
    constexpr bool operator>(const NewType& rhs) const {              \
      return val_ > rhs.val_;                                         \
    }                                                                 \
    constexpr bool operator<=(const NewType& rhs) const {             \
      return val_ <= rhs.val_;                                        \
    }                                                                 \
    constexpr bool operator>=(const NewType& rhs) const {             \
      return val_ >= rhs.val_;                                        \
    }                                                                 \
    struct Hash {                                                     \
      constexpr size_t operator()(const NewType& val) const {         \
        return val.val_;                                              \
      }                                                               \
    };                                                                \
  } __attribute__((__packed__))
