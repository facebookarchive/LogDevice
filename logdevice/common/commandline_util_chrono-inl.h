/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cinttypes>
#include <string>
#include <vector>

#include <folly/String.h>

#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

// This is the workhorse function, converts a string like "10s" to the target
// duration class.
template <class DurationClass>
int parse_chrono_string(const std::string& str, DurationClass* duration_out) {
  using namespace std::chrono;

  if (str == "max") {
    *duration_out = DurationClass::max();
    return 0;
  }
  if (str == "min") {
    *duration_out = DurationClass::min();
    return 0;
  }

  // Using a long double because it has enough precision to represent all
  // 64-bit ints
  long double dval;
  char suffix_buf[16];
  int nread = sscanf(str.c_str(), "%Lf%15s", &dval, suffix_buf);

  if (nread == 1 && dval == 0) {
    // Zero is fine without a suffix. If it does have a suffix, however,
    // we proceed to check that the suffix is valid.
    *duration_out = DurationClass(0);
    return 0;
  }

  if (nread != 2) {
    err = E::INVALID_PARAM;
    return -1;
  }
  std::string suffix(suffix_buf);

  DurationClass output;
  if (suffix == "ns") {
    duration<long double, std::nano> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "us") {
    duration<long double, std::micro> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "ms") {
    duration<long double, std::milli> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "s") {
    duration<long double> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "min" || suffix == "mins") {
    duration<long double, std::ratio<60>> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "h" || suffix == "hr" || suffix == "hrs") {
    duration<long double, std::ratio<3600>> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "d" || suffix == "day" || suffix == "days") {
    duration<long double, std::ratio<86400>> src(dval);
    output = duration_cast<DurationClass>(src);
  } else if (suffix == "w" || suffix == "week" || suffix == "weeks") {
    duration<long double, std::ratio<604800>> src(dval);
    output = duration_cast<DurationClass>(src);
  } else {
    err = E::INVALID_PARAM;
    return -1;
  }

  if (output.count() == 0 && dval != 0) {
    // Avoid the pitfall of upcasting a value that is too small, e.g. parsing
    // "1ms" as seconds, which truncates to zero.
    err = E::TRUNCATED;
    return -1;
  }

  *duration_out = output;
  return 0;
}

template <class DurationClass>
std::string format_chrono_string(DurationClass duration) {
  if (duration == DurationClass::min()) {
    return "min";
  }
  if (duration == DurationClass::max()) {
    return "max";
  }

  static_assert(std::ratio_divide<typename DurationClass::period,
                                  std::chrono::nanoseconds::period>::den == 1,
                "Duration type must be divisible by nanoseconds.");
  static_assert(
      std::ratio_greater_equal<typename DurationClass::period,
                               std::chrono::nanoseconds::period>::value,
      "Duration unit must be no smaller than nanoseconds.");

  // C++ < 20 doesn't have these.
  using weeks = std::chrono::duration<int64_t, std::ratio<604800>>;
  using days = std::chrono::duration<int64_t, std::ratio<86400>>;

#define U(u, name)                                                           \
  if ((duration % u(1)).count() == 0) {                                      \
    return std::to_string(std::chrono::duration_cast<u>(duration).count()) + \
        name;                                                                \
  }

  U(weeks, "w")
  U(days, "d")
  U(std::chrono::hours, "h")
  U(std::chrono::minutes, "min")
  U(std::chrono::seconds, "s")
  U(std::chrono::milliseconds, "ms")
  U(std::chrono::microseconds, "us")
  U(std::chrono::nanoseconds, "ns")
#undef U

  // We checked that duration type is divisible by nanoseconds.
  // So, if nothing else, we must have been able to format it in nanoseconds.
  ld_check(false);
  return "?";
}

namespace detail {
inline const char* chrono_suffix(std::chrono::nanoseconds) {
  return "ns";
}
inline const char* chrono_suffix(std::chrono::microseconds) {
  return "us";
}
inline const char* chrono_suffix(std::chrono::milliseconds) {
  return "ms";
}
inline const char* chrono_suffix(std::chrono::seconds) {
  return "s";
}
inline const char* chrono_suffix(std::chrono::minutes) {
  return "min";
}
inline const char* chrono_suffix(std::chrono::hours) {
  return "hr";
}
} // namespace detail

template <class DurationClass>
std::string chrono_string(DurationClass duration) {
  return std::to_string(duration.count()) + detail::chrono_suffix(duration);
}

// Simple wrapper around boost::program_options::value() that also sets the
// default text.  If we didn't try to call default_value() with the second
// param, boost::program_options would try to lexical_cast the chrono class to
// a string, which would fail.
template <class DurationClass>
boost::program_options::typed_value<DurationClass>*
chrono_value(DurationClass* duration_out, bool default_value) {
  auto ret = boost::program_options::value<DurationClass>(duration_out);
  if (default_value) {
    ret->default_value(*duration_out, chrono_string(*duration_out));
  }
  return ret;
}

template <class DurationClass>
boost::program_options::typed_value<chrono_interval_t<DurationClass>>*
chrono_interval_value(chrono_interval_t<DurationClass>* interval_out,
                      bool default_value) {
  auto ret = boost::program_options::value<chrono_interval_t<DurationClass>>(
      interval_out);
  if (default_value) {
    ret->default_value(*interval_out,
                       folly::to<std::string>(chrono_string(interval_out->lo),
                                              "..",
                                              chrono_string(interval_out->hi)));
  }
  return ret;
}

template <class DurationClass>
boost::program_options::typed_value<chrono_expbackoff_t<DurationClass>>*
chrono_expbackoff_value(chrono_expbackoff_t<DurationClass>* out,
                        bool default_value) {
  auto ret =
      boost::program_options::value<chrono_expbackoff_t<DurationClass>>(out);
  if (default_value) {
    if (out->multiplier !=
        chrono_expbackoff_t<DurationClass>::DEFAULT_MULTIPLIER) {
      ret->default_value(
          *out,
          folly::to<std::string>(chrono_string(out->initial_delay),
                                 "..",
                                 chrono_string(out->max_delay),
                                 "-",
                                 folly::to<std::string>(out->multiplier),
                                 "x"));
    } else {
      ret->default_value(
          *out,
          folly::to<std::string>(chrono_string(out->initial_delay),
                                 "..",
                                 chrono_string(out->max_delay)));
    }
  }
  return ret;
}

namespace detail {

template <class DurationClass, class charT>
void chrono_validate(boost::any& v,
                     const std::vector<std::basic_string<charT>>& xs) {
  using namespace boost::program_options;
  validators::check_first_occurrence(v);
  std::basic_string<charT> str(validators::get_single_string(xs));

  DurationClass output;
  int rv = parse_chrono_string(str.c_str(), &output);
  if (rv != 0) {
    if (err == E::TRUNCATED) {
      throw error("time parameter \"" + str +
                  "\" would get truncated to zero "
                  "because target variable is coarser");
    } else {
      throw validation_error(validation_error::invalid_option_value);
    }
  }

  v = boost::any(output);
}

template <class DurationClass, class charT>
chrono_interval_t<DurationClass>
parse_chrono_interval(const std::basic_string<charT>& str) {
  using namespace boost::program_options;
  chrono_interval_t<DurationClass> output;
  std::vector<std::string> pieces;
  folly::split("..", str, pieces);

  if (pieces.size() < 1 || pieces.size() > 2) {
    throw validation_error(validation_error::invalid_option_value);
  }

  DurationClass* args[] = {&output.lo, &output.hi};
  for (size_t i = 0; i < pieces.size(); ++i) {
    int rv = parse_chrono_string(pieces[i].c_str(), args[i]);
    if (rv != 0) {
      if (err == E::TRUNCATED) {
        throw error("time parameter \"" + pieces[0] +
                    "\" would get truncated "
                    "to zero because target variable is coarser");
      } else {
        throw validation_error(validation_error::invalid_option_value);
      }
    }
  }

  if (pieces.size() == 1) {
    output.hi = output.lo;
  }
  return output;
}

template <class DurationClass, class charT>
void chrono_interval_validate(boost::any& v,
                              const std::vector<std::basic_string<charT>>& xs) {
  using namespace boost::program_options;
  typedef std::basic_string<charT> StringT;

  validators::check_first_occurrence(v);
  StringT str(validators::get_single_string(xs));
  chrono_interval_t<DurationClass> output(
      parse_chrono_interval<DurationClass>(str));
  v = boost::any(output);
}

template <class DurationClass, class charT>
void chrono_expbackoff_validate(
    boost::any& v,
    const std::vector<std::basic_string<charT>>& xs) {
  using namespace boost::program_options;
  typedef std::basic_string<charT> StringT;
  typedef chrono_expbackoff_t<DurationClass> ExpBackoff;

  validators::check_first_occurrence(v);
  StringT str(validators::get_single_string(xs));

  // split input string into interval and multiplier parts
  std::vector<StringT> pieces;
  folly::split("-", str, pieces);
  if (pieces.size() < 1 || pieces.size() > 2) {
    throw validation_error(validation_error::invalid_option_value);
  }

  // validate interval part
  chrono_interval_t<DurationClass> interval(
      parse_chrono_interval<DurationClass>(pieces[0]));
  ExpBackoff output(interval.lo, interval.hi);

  // validate multiplier part, if any
  if (pieces.size() == 2) {
    StringT& exp_str = pieces[1];
    if (exp_str.length() < 2) {
      throw validation_error(validation_error::invalid_option_value);
    }
    char last_char = exp_str[exp_str.length() - 1];
    if (last_char != 'x' && last_char != 'X') {
      throw validation_error(validation_error::invalid_option_value);
    }
    exp_str.resize(exp_str.length() - 1);
    try {
      output.multiplier = folly::to<decltype(output.multiplier)>(exp_str);
    } catch (const folly::ConversionError&) {
      throw validation_error(validation_error::invalid_option_value);
    }
  }

  v = boost::any(output);
}

} // namespace detail
}} // namespace facebook::logdevice

// Here we specialize the validate() global function for chrono types in the
// std namespace.  Without this, boost would be trying to use its default
// validate() method which attempts a lexical_cast.
#define VALIDATE_CHRONO_TYPE(chrono_type)                                      \
  template <typename charT>                                                    \
  inline void validate(boost::any& v,                                          \
                       const std::vector<std::basic_string<charT>>& xs,        \
                       chrono_type*,                                           \
                       int) {                                                  \
    facebook::logdevice::detail::chrono_validate<chrono_type, charT>(v, xs);   \
  }                                                                            \
  template <typename charT>                                                    \
  inline void validate(boost::any& v,                                          \
                       const std::vector<std::basic_string<charT>>& xs,        \
                       facebook::logdevice::chrono_interval_t<chrono_type>*,   \
                       int) {                                                  \
    facebook::logdevice::detail::chrono_interval_validate<chrono_type, charT>( \
        v, xs);                                                                \
  }                                                                            \
  template <typename charT>                                                    \
  inline void validate(boost::any& v,                                          \
                       const std::vector<std::basic_string<charT>>& xs,        \
                       facebook::logdevice::chrono_expbackoff_t<chrono_type>*, \
                       int) {                                                  \
    facebook::logdevice::detail::chrono_expbackoff_validate<chrono_type,       \
                                                            charT>(v, xs);     \
  }

namespace std { namespace chrono {
VALIDATE_CHRONO_TYPE(std::chrono::nanoseconds)
VALIDATE_CHRONO_TYPE(std::chrono::microseconds)
VALIDATE_CHRONO_TYPE(std::chrono::milliseconds)
VALIDATE_CHRONO_TYPE(std::chrono::seconds)
VALIDATE_CHRONO_TYPE(std::chrono::minutes)
VALIDATE_CHRONO_TYPE(std::chrono::hours)
}} // namespace std::chrono

#undef VALIDATE_CHRONO_TYPE
