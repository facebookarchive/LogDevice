/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <initializer_list>

#include <folly/Likely.h>
#include <folly/Portability.h>

namespace facebook { namespace logdevice { namespace dbg {

/**
 * @file Various assert()-like macros. These should be used instead of assert()
 * in logdevice code, except for API headers and examples. Summary:
 *
 * +-----------------+----------------------+------------------+
 * |                 | evaluated if !NDEBUG | crash on failure |
 * +-----------------+----------------------+------------------+
 * | ld_assert()     |          no          |        yes       |
 * +-----------------+----------------------+------------------+
 * | ld_check()      |          yes         |        yes (*)   |
 * +-----------------+----------------------+------------------+
 * | ld_catch()  (**)|          yes         |       maybe      |
 * +-----------------+----------------------+------------------+
 * | dd_assert() (**)|          yes         |   only in tests  |
 * +-----------------+----------------------+------------------+
 *
 * Use ld_check() or ld_assert() for conditions that are always true unless
 * there's a bug.
 * Use dd_assert() for conditions that depend on data from disk or network, but
 * don't depend on logdevice version.
 * Don't use ld_catch().
 *
 * ld_check() is for expressions that are cheap to evaluate, or evaluated
 * infrequently.
 *
 * (*)  There's a setting to make ld_check() not crash (abort-on-failed-check),
 *      but it's reserved for emergencies. When using ld_check() you can pretend
 *      that it always crashes on failure.
 * (**) ld_catch() and dd_assert() are declared in debug.h
 */

enum class CheckType {
  CHECK,
  ASSERT,
};

// Defined in debug.cpp
void ld_check_fail_impl(CheckType type,
                        const char* expr,
                        const char* component,
                        const char* function,
                        const int line);

#define ld_check_base(expr, precheck, type)                           \
  do {                                                                \
    if ((precheck) && UNLIKELY(!(expr))) {                            \
      ld_check_fail_impl(::facebook::logdevice::dbg::CheckType::type, \
                         #expr,                                       \
                         __FILE__,                                    \
                         __FUNCTION__,                                \
                         __LINE__);                                   \
    }                                                                 \
  } while (false)

#define ld_check_base_op(op, x, y, precheck, type)                         \
  do {                                                                     \
    auto val1{x};                                                          \
    auto val2{y};                                                          \
    if ((precheck) && UNLIKELY(!(val1 op val2))) {                         \
      ld_check_fail_impl(::facebook::logdevice::dbg::CheckType::type,      \
                         (std::string(#x) + " " #op " " #y " (" +          \
                          ::facebook::logdevice::toString(val1) + " vs " + \
                          ::facebook::logdevice::toString(val2) + ")")     \
                             .c_str(),                                     \
                         __FILE__,                                         \
                         __FUNCTION__,                                     \
                         __LINE__);                                        \
    }                                                                      \
  } while (false)

#define ld_check_base_between(x, min, max, precheck, type)                  \
  do {                                                                      \
    auto val{x};                                                            \
    auto min_val{min};                                                      \
    auto max_val{max};                                                      \
    if ((precheck) && UNLIKELY((val) < (min_val) || (val) > (max_val))) {   \
      ld_check_fail_impl(::facebook::logdevice::dbg::CheckType::type,       \
                         (std::string(#min " <= " #x " <= " #max " (") +    \
                          ::facebook::logdevice::toString(val) + " vs [" +  \
                          ::facebook::logdevice::toString(min_val) + ", " + \
                          ::facebook::logdevice::toString(max_val) + "])")  \
                             .c_str(),                                      \
                         __FILE__,                                          \
                         __FUNCTION__,                                      \
                         __LINE__);                                         \
    }                                                                       \
  } while (false)

//////////  Check: Conditions that are evaulated in non-debug, as well as debug
//////////  builds, and abort() when they fail.

//  Make sure these are cheap to evaluate, e.g. comparing a primitive value that
//  you just computed, or are about to use to a compile time constant.

#define ld_check(expr) ld_check_base(expr, true, CHECK)

// To use these you need to #include "logdevice/common/toString.h"
// This file doesn't include it to keep it lightweight when these are not used.
#define ld_check_op(op, x, y) ld_check_base_op(op, x, y, true, CHECK)
#define ld_check_eq(x, y) ld_check_op(==, x, y)
#define ld_check_ne(x, y) ld_check_op(!=, x, y)
#define ld_check_le(x, y) ld_check_op(<=, x, y)
#define ld_check_lt(x, y) ld_check_op(<, x, y)
#define ld_check_ge(x, y) ld_check_op(>=, x, y)
#define ld_check_gt(x, y) ld_check_op(>, x, y)
#define ld_check_between(x, min, max) \
  ld_check_base_between(x, min, max, true, CHECK)

//////////  Assert: Conditions that are only evaulated in debug builds.

//  Use for asserts that take a lot of resources to evaluate (run time, memory,
//  cache misses, etc.)

#define ld_assert(expr) ld_check_base(expr, ::folly::kIsDebug, ASSERT)

// To use these you need to #include "logdevice/common/toString.h"
#define ld_assert_op(op, x, y) \
  ld_check_base_op(op, x, y, ::folly::kIsDebug, ASSERT)
#define ld_assert_eq(x, y) ld_assert_op(==, x, y)
#define ld_assert_ne(x, y) ld_assert_op(!=, x, y)
#define ld_assert_le(x, y) ld_assert_op(<=, x, y)
#define ld_assert_lt(x, y) ld_assert_op(<, x, y)
#define ld_assert_ge(x, y) ld_assert_op(>=, x, y)
#define ld_assert_gt(x, y) ld_assert_op(>, x, y)
#define ld_assert_between(x, min, max) \
  ld_check_base_between(x, min, max, ::folly::kIsDebug, ASSERT)

template <typename T>
struct CheckInHelper {
  T& target;
  CheckInHelper(T& t) : target(t) {}
  bool isIn(std::initializer_list<T> mylist) {
    for (auto it = mylist.begin(); it != mylist.end(); ++it) {
      if (*it == target) {
        return true;
      }
    }
    return false;
  }
};

// mylist must be enclosed in parens, like this:
//   ld_check_in(state_, ({State::MUTATION, State::CLEAN}));
#define ld_check_in(target, mylist)                                           \
  do {                                                                        \
    auto target1{target};                                                     \
    if (UNLIKELY(!facebook::logdevice::dbg::CheckInHelper<decltype(target1)>( \
                      target1)                                                \
                      .isIn mylist)) {                                        \
      ld_check_fail_impl(                                                     \
          ::facebook::logdevice::dbg::CheckType::CHECK,                       \
          (std::string(#target) + " (" +                                      \
           ::facebook::logdevice::toString(target1) + ") in " #mylist)        \
              .c_str(),                                                       \
          __FILE__,                                                           \
          __FUNCTION__,                                                       \
          __LINE__);                                                          \
    }                                                                         \
  } while (false)

}}} // namespace facebook::logdevice::dbg
