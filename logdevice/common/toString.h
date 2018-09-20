/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <iterator>
#include <ostream>
#include <sstream>
#include <tuple>
#include <type_traits>

#include <folly/Conv.h>

namespace facebook { namespace logdevice {

// toString(const T&) formats a value of type T into a human-readable string.
// Used mostly for debugging.
// E.g. an int would be formatted as in "42",
// an std::map<std::string, std::vector<NodeID>> would be formatted as in
// "{(key1, [N3:1, N1:2]), (key2, [])}". T should:
//  1. have toString() method, or
//  2. have operator<<(ostream&, const T&), or
//  3. be an std::pair or std::tuple, or
//  4. be a container (with begin() and end()) with elements of type
//     eligible for toString(), or
//  5. (feel free to add your own specialization here).

/**
 * Some helpers to detect things about the class we're dealing with.
 */

namespace to_string_detail {

// Since C++17 std::void_t will be in standard library, and this definition
// won't be needed.
template <typename... Ts>
struct make_void {
  typedef void type;
};
template <typename... Ts>
using void_t = typename make_void<Ts...>::type;

// has_to_string_method<T>::value is true iff T has toString() method.
template <typename, typename = void_t<>>
struct has_to_string_method {
  static constexpr bool value = false;
};
template <typename T>
struct has_to_string_method<
    T,
    void_t<decltype(std::declval<T const>().toString())>> {
  static constexpr bool value = true;
};

// has_val_method<T>::value is true iff T has val() method.
template <typename, typename = void_t<>>
struct has_val_method {
  static constexpr bool value = false;
};
template <typename T>
struct has_val_method<T, void_t<decltype(std::declval<T const>().val())>> {
  static constexpr bool value = true;
};

// has_value_method<T>::value is true iff T has value() method.
template <typename, typename = void_t<>>
struct has_value_method {
  static constexpr bool value = false;
};
template <typename T>
struct has_value_method<T, void_t<decltype(std::declval<T const>().value())>> {
  static constexpr bool value = true;
};

// has_hasValue_method<T>::value is true iff T has hasvalue() method.
template <typename, typename = void_t<>>
struct has_hasValue_method {
  static constexpr bool value = false;
};
template <typename T>
struct has_hasValue_method<
    T,
    void_t<decltype(std::declval<T const>().hasValue())>> {
  static constexpr bool value = true;
};

// is_ostreamable<T>::value is true iff T can be written to an std::ostream
// with operator <<.
template <typename, typename = void_t<>>
struct is_ostreamable {
  static constexpr bool value = false;
};
template <typename T>
struct is_ostreamable<T,
                      void_t<decltype(std::declval<std::ostream&>()
                                      << std::declval<T const>())>> {
  static constexpr bool value = true;
};

// is_container<T>::value is true iff T is an iterable container.
template <typename, typename = void_t<>>
struct is_container {
  static constexpr bool value = false;
};
template <typename T>
struct is_container<T,
                    void_t<decltype(std::begin(std::declval<T const>())),
                           decltype(std::end(std::declval<T const>()))>> {
  static constexpr bool value = true;
};

// If T is a container (is_container), is_container_associative<T>::value
// is true iff T is an _associative_ container,
// e.g. std::set, std::map, std::unordered_map, std::multiset, SmallMap.
template <typename, typename = void_t<>>
struct is_container_associative {
  static constexpr bool value = false;
};
template <typename T>
struct is_container_associative<T, void_t<typename T::key_type>> {
  static constexpr bool value = true;
};

} // namespace to_string_detail

/**
 * Some forward declarations.
 * Uninteresting, copy-pasted from definitions (below).
 */

template <typename A, typename B>
std::string toString(const std::pair<A, B>& x);

template <typename It>
std::string rangeToString(It begin, It end);

template <
    typename T,
    typename std::enable_if<
        to_string_detail::is_container_associative<T>::value &&
        to_string_detail::is_container<T>::value &&
        !to_string_detail::is_ostreamable<T>::value &&
        !to_string_detail::has_to_string_method<T>::value>::type* = nullptr>
std::string toString(const T& x);

template <
    typename T,
    typename std::enable_if<
        to_string_detail::is_container<T>::value &&
        !to_string_detail::is_container_associative<T>::value &&
        !to_string_detail::is_ostreamable<T>::value &&
        !to_string_detail::has_to_string_method<T>::value>::type* = nullptr>
std::string toString(const T& x);

/**
 * Definitions of various toString() overloads.
 */

// T has toString() method.
template <typename T,
          typename std::enable_if<to_string_detail::has_to_string_method<
              T>::value>::type* = nullptr>
std::string toString(const T& x) {
  return x.toString();
}

// T can be written to std::ostream (and doesn't have toString() method).
template <typename T,
          typename std::enable_if<to_string_detail::is_ostreamable<T>::value &&
                                  !to_string_detail::has_to_string_method<
                                      T>::value>::type* = nullptr>
std::string toString(const T& x) {
  std::ostringstream ss;
  ss << x;
  return ss.str();
}

/**
 * toString() for any LOGDEVICE_STRONG_TYPEDEF().  Actually, for anything with a
 * public val() method.  You can override this by providing a concrete
 * definition, see the logid_t version in util.h for an example.
 */
template <typename T,
          typename std::enable_if<to_string_detail::has_val_method<T>::value &&
                                  !to_string_detail::is_container<T>::value &&
                                  !to_string_detail::is_ostreamable<T>::value &&
                                  !to_string_detail::has_to_string_method<
                                      T>::value>::type* = nullptr>
std::string toString(const T& val_thing) {
  return folly::to<std::string>(val_thing.val());
}

/**
 * toString() for any "folly::Optional like" class. You can override this by
 * providing a concrete definition.
 */
template <
    typename T,
    typename std::enable_if<
        to_string_detail::has_value_method<T>::value &&
        to_string_detail::has_hasValue_method<T>::value &&
        !to_string_detail::is_container<T>::value &&
        !to_string_detail::is_ostreamable<T>::value &&
        !to_string_detail::has_to_string_method<T>::value>::type* = nullptr>
std::string toString(const T& optional_thing) {
  if (optional_thing.hasValue()) {
    return toString(optional_thing.value());
  }
  return "none";
}

/**
 * toString() for nullptr_t.  The current standard doesn't support operator<<
 * with an expression of type nullptr_t.
 */
inline std::string toString(std::nullptr_t /* value */) {
  return "nullptr";
}

namespace to_string_detail {

template <size_t n, typename... T>
struct tupleSuffixToString {
  static std::string format(const std::tuple<T...>& t) {
    return toString(std::get<sizeof...(T) - n>(t)) + (n > 1 ? ", " : "") +
        tupleSuffixToString<n - 1, T...>::format(t);
  }
};
template <typename... T>
struct tupleSuffixToString<0, T...> {
  static std::string format(const std::tuple<T...>& /*t*/) {
    return "";
  }
};

} // namespace to_string_detail

// std::tuple, without surrounding '(' and ')'.
template <typename... T>
std::string tupleToString(const std::tuple<T...>& t) {
  return to_string_detail::tupleSuffixToString<sizeof...(T), T...>::format(t);
}

// std::tuple
template <typename... T>
std::string toString(const std::tuple<T...>& t) {
  return "(" + tupleToString(t) + ")";
}

// std::pair
template <typename A, typename B>
std::string toString(const std::pair<A, B>& x) {
  return "(" + toString(x.first) + ", " + toString(x.second) + ")";
}

// Iterable range, without surrounding '['/'{' and ']'/'}'.
template <typename It>
std::string rangeToString(It begin, It end) {
  std::string s;
  bool f = true;
  for (; begin != end; ++begin) {
    if (!f) {
      s += ", ";
    }
    f = false;
    s += toString(*begin);
  }
  return s;
}

// Associative containers.
template <typename T,
          typename std::enable_if<
              to_string_detail::is_container_associative<T>::value &&
              to_string_detail::is_container<T>::value &&
              !to_string_detail::is_ostreamable<T>::value &&
              !to_string_detail::has_to_string_method<T>::value>::type*>
std::string toString(const T& x) {
  return "{" + rangeToString(x.begin(), x.end()) + "}";
}

// Non-associative containers.
template <typename T,
          typename std::enable_if<
              to_string_detail::is_container<T>::value &&
              !to_string_detail::is_container_associative<T>::value &&
              !to_string_detail::is_ostreamable<T>::value &&
              !to_string_detail::has_to_string_method<T>::value>::type*>
std::string toString(const T& x) {
  return "[" + rangeToString(x.begin(), x.end()) + "]";
}

}} // namespace facebook::logdevice
