/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>
#include <type_traits>

#include <boost/icl/interval_set.hpp>
#include <folly/Format.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * @file  Timestamp, a wrapper around std::chrono::time_point.
 *
 *  Manipulating time_points directly can be so cumbersome that often
 *  raw durations are used, even though durations lose the ability
 *  to validate that all variables in an operation were derived from
 *  the same reference clock. Timestamp reduces the amount of template
 *  boiler plate required so that Timestamp is almost as easy
 *  to use as std::chrono::duration.  Where it is more verbose, the
 *  explicit conversions required increase readability and/or clarify
 *  developer intent.
 *
 *  Unlike std::chrono::time_point, Timestamp can be instantiated to
 *  use atomic storage.  In an atomic configuration, Timestamp assumes
 *  it must only guarantee the coherency of its own data and is not being
 *  used to provide acquire/release barrier semantics (i.e. reads and
 *  writes to an atomic Timestamp are not used to synchronize the updates
 *  of other data between threads).  For this reason, many operations use
 *  std::memory_order_relaxed.
 */

namespace detail {

/**
 * Storage for non-atomic Timestamps.
 */
template <typename T>
class Holder {
 public:
  explicit Holder(T value) : storage_(value) {}

  T load(std::memory_order /*order*/ = std::memory_order_seq_cst) const {
    return storage_;
  }

  void store(T desired,
             std::memory_order /*order*/ = std::memory_order_seq_cst) {
    storage_ = desired;
  }

  /**
   * Store the maximum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  T storeMax(T value) {
    T observed = storage_;
    storage_ = std::max(storage_, value);
    return observed;
  }

  /**
   * Store the minimum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  T storeMin(T value) {
    T observed = storage_;
    storage_ = std::min(storage_, value);
    return observed;
  }

  /**
   * Store value if "f(current_value, arg)" returns true.
   *
   * @return  The previously stored value before any modification.
   */
  template <typename F>
  T storeConditional(const T value, const T arg, F f) {
    T observed = storage_;
    if (f(storage_, arg)) {
      storage_ = value;
    }
    return observed;
  }

  /**
   * Store current_value + value.
   *
   * @return  The previously stored value before any modification.
   */
  T fetchAdd(const T value,
             std::memory_order /*order*/ = std::memory_order_seq_cst) {
    T observed = storage_;
    storage_ += value;
    return observed;
  }

  /**
   * Store current_value - value.
   *
   * @return  The previously stored value before any modification.
   */
  T fetchSub(const T value,
             std::memory_order /*order*/ = std::memory_order_seq_cst) {
    T observed = storage_;
    storage_ -= value;
    return observed;
  }

 private:
  T storage_;
};

/**
 * Storage for atomic Timestamps.
 */
template <typename T>
class AtomicHolder {
 public:
  explicit AtomicHolder(T value) : storage_(value) {}

  T load(std::memory_order order = std::memory_order_seq_cst) const {
    return storage_.load(order);
  }

  void store(T desired, std::memory_order order = std::memory_order_seq_cst) {
    storage_.store(desired, order);
  }

  /**
   * Store the maximum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  T storeMax(T value) {
    return atomic_fetch_max(storage_, value);
  }

  /**
   * Store the minimum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  T storeMin(T value) {
    return atomic_fetch_min(storage_, value);
  }

  /**
   * Store value if "f(current_value, arg)" returns true.
   *
   * @return  The previously stored value before any modification.
   */
  template <typename F>
  T storeConditional(const T value, const T arg, F f) {
    return atomic_conditional_store(storage_, value, arg, f);
  }

  /**
   * Store current_value + value.
   *
   * @return  The previously stored value before any modification.
   */
  T fetchAdd(const T value,
             std::memory_order order = std::memory_order_seq_cst) {
    // Unfortunately fetch_add() is not defined for atomic<chrono::duration>.
    T observed = storage_.load(order);
    while (!storage_.compare_exchange_weak(observed, observed + value)) {
    }
    return observed;
  }

  /**
   * Store current_value - value.
   *
   * @return  The previously stored value before any modification.
   */
  T fetchSub(const T value,
             std::memory_order order = std::memory_order_seq_cst) {
    T observed = storage_.load(order);
    while (!storage_.compare_exchange_weak(observed, observed - value)) {
    }
    return observed;
  }

 private:
  std::atomic<T> storage_;
};

} // namespace detail

template <typename Clock,
          template <typename> class DurationHolder,
          typename Duration = typename Clock::duration>
class Timestamp {
  // Allow access between all instantiations of Timestamp. This simplifies
  // the implementation of copy and conversion logic.
  template <typename C, template <typename> class H, typename D>
  friend class Timestamp;

 public:
  // Emulate types exported by std::chrono::time_point
  using clock = Clock;
  using duration = Duration;
  using rep = typename Duration::rep;
  using period = typename Duration::period;

  using TimePoint = std::chrono::time_point<Clock, Duration>;

  // Always pass out a non-atomic result. This avoids any additional
  // atomic operations for temporaries and is compatible with reception
  // into either an atomic or non-atomic Timestamp.
  template <typename D>
  using OutTimestamp = Timestamp<Clock, detail::Holder, D>;

  /**
   * Allow explicit instantiation from any duration type. Since the
   * construction is explicit, the desired clock and duration are
   * obvious to the reader, so there is no value in requiring a
   * duration cast.
   */
  template <typename D = Duration>
  explicit Timestamp(D d = D::min()) : duration_(to<Duration>(d)) {}

  /**
   * Allow implicit conversion from any std::chrono::time_point that
   * uses the same clock.
   */
  template <typename D, template <typename, typename> class TimePointT>
  /* implicit */ Timestamp(TimePointT<Clock, D> tp)
      : duration_(to<Duration>(tp.time_since_epoch())) {}

  /**
   * Allow explicit instantiation from any Timestamp that uses the same
   * clock.
   */
  template <template <typename> class H, class D>
  Timestamp(const Timestamp<Clock, H, D>& src)
      : duration_(to<Duration>(src.duration_.load())) {}

  /**
   * Allow implicit conversion to any std::chrono::time_point that
   * uses the same clock.
   */
  template <typename D>
  /* implicit */ operator std::chrono::time_point<Clock, D>() const {
    return std::chrono::time_point<Clock, D>(to<D>());
  }

  /** Convenience function for external operator functions. */
  TimePoint timePoint() const {
    return TimePoint(*this);
  }

  // ---------------- Conversion to other time resolutions --------------------
  template <typename D>
  D to() const {
    return to<D>(duration_.load());
  }
  std::chrono::microseconds toMicroseconds() const {
    return to<std::chrono::microseconds>();
  }
  std::chrono::milliseconds toMilliseconds() const {
    return to<std::chrono::milliseconds>();
  }
  std::chrono::seconds toSeconds() const {
    return to<std::chrono::seconds>();
  }
  std::chrono::minutes toMinutes() const {
    return to<std::chrono::minutes>();
  }
  std::chrono::hours toHours() const {
    return to<std::chrono::hours>();
  }

  // -------------------- Emulation of time_point methods. --------------------
  Duration time_since_epoch() const {
    return duration_.load();
  }

  template <template <typename> class H>
  OutTimestamp<Duration> operator=(const Timestamp<Clock, H, Duration>& rhs) {
    return operator=(rhs.duration_.load());
  }

  template <typename D>
  OutTimestamp<Duration>
  operator=(const std::chrono::time_point<Clock, D>& rhs) {
    return operator=(rhs.time_since_epoch());
  }

  template <template <typename> class H>
  OutTimestamp<Duration> operator=(TimePoint rhs) {
    return operator=(rhs.time_since_epoch());
  }

  OutTimestamp<Duration> operator+=(const Duration& d) {
    Duration observed = duration_.fetchAdd(d);
    return OutTimestamp<Duration>(observed + d);
  }

  OutTimestamp<Duration> operator-=(const Duration& d) {
    Duration observed = duration_.fetchSub(d);
    return OutTimestamp<Duration>(observed - d);
  }

  OutTimestamp<Duration> operator++() {
    Duration observed = duration_.fetchAdd(Duration(1));
    return OutTimestamp<Duration>(++observed);
  }

  OutTimestamp<Duration> operator--() {
    Duration observed = duration_.fetchSub(Duration(1));
    return OutTimestamp<Duration>(--observed);
  }

  OutTimestamp<Duration> operator++(int) {
    Duration observed = duration_.fetchAdd(Duration(1));
    return OutTimestamp<Duration>(observed);
  }

  OutTimestamp<Duration> operator--(int) {
    Duration observed = duration_.fetchSub(Duration(1));
    return OutTimestamp<Duration>(observed);
  }

  // ------------------------- "Atomic" Functions -----------------------------
  // NOTE: Methods are only atomic for Timestamp types that use AtomicHolder
  void store(TimePoint value) {
    duration_.store(value.time_since_epoch());
  }

  /**
   * Store the maximum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  OutTimestamp<Duration> storeMax(TimePoint value) {
    return OutTimestamp<Duration>(duration_.storeMax(value.time_since_epoch()));
  }

  /**
   * Store the minimum of the currently stored value and the passed in value.
   *
   * @return  The previously stored value before any modification.
   */
  OutTimestamp<Duration> storeMin(TimePoint value) {
    return OutTimestamp<Duration>(duration_.storeMin(value.time_since_epoch()));
  }

  /**
   * Store value if "f(current_value, arg)" returns true.
   *
   * @return  The previously stored value before any modification.
   */
  template <typename F>
  OutTimestamp<Duration> storeConditional(const TimePoint value,
                                          const TimePoint arg,
                                          F f) {
    return OutTimestamp<Duration>(duration_.storeConditional(
        value.time_since_epoch(), arg.time_since_epoch(), [&f](auto a, auto b) {
          return f(TimePoint(a), TimePoint(b));
        }));
  }

  /**
   * Formats the timestamps into a std::string.
   * Only defined for Timestamp derived from system clock for now, because it's
   * not clear how to format steady_clock timestamps in a nice
   * and non-misleading way.
   */
  template <typename Clock2 = Clock> // just SFINAE things
  typename std::enable_if<
      std::is_same<Clock2, std::chrono::system_clock>::value,
      std::string>::type
  toString() const;

  /**
   * Convert steady timestamp to system timestamp. Pseudocode:
   * system_clock::now() + (*this - steady_clock::now())
   * The result will have a small random variation from call to call, and
   * will jump if system clock is adjusted (e.g. daylight savings or leap
   * seconds).
   */
  template <typename Clock2 = Clock> // just SFINAE things
  typename std::enable_if<
      std::is_same<Clock2, std::chrono::steady_clock>::value,
      Timestamp<std::chrono::system_clock, DurationHolder, Duration>>::type
  approximateSystemTimestamp() const;

  /**
   * The current time on the clock reference for this Timestamp type.
   */
  static OutTimestamp<Duration> now() {
    return Clock::now();
  }

  /**
   * Construct a Timestamp from a duration performing any required
   * duration cast.
   */
  template <typename D>
  static OutTimestamp<Duration> from(D other) {
    return OutTimestamp<Duration>(to<Duration>(other));
  }

  static constexpr OutTimestamp<Duration> min() {
    return TimePoint::min();
  }
  static constexpr OutTimestamp<Duration> max() {
    return TimePoint::max();
  }
  static constexpr OutTimestamp<Duration> zero() {
    return OutTimestamp<Duration>(Duration::zero());
  }

  static bool fromString(std::string tstr, OutTimestamp<Duration>& result) {
    auto failed_conversion = [&]() {
      ld_warning("Unabled to convert %s to Timestamp.", tstr.c_str());
      return false;
    };

    // Treat underscores as spaces. This allows specifying timestamps without
    // spaces, e.g. 2019-05-13_10:00:00, which is sometimes convenient to avoid
    // cumbersome quotes escaping on command line
    // (e.g. `echo ... --time-from=2019-05-13_10:18:00`
    //  instead of `echo ... --time-from=\"2019-05-13 10:18:00\"`).
    for (char& c : tstr) {
      if (c == '_') {
        c = ' ';
      }
    }

    // Remove leading and trailing whitespace.
    strip_string(tstr);

    // Range limits shorthand
    if (tstr == "-inf") {
      result = OutTimestamp<Duration>::min();
      return true;
    }
    if (tstr == "+inf") {
      result = OutTimestamp<Duration>::max();
      return true;
    }

    // YYYY-MM-DD HH:MM:SS[.mmm] (machine local time).
    struct tm tm {};
    intmax_t epoch_ms;
    const char* ms_start = strptime(tstr.c_str(), "%F %T", &tm);
    if (ms_start != nullptr) {
      // strptime was successful.
      tm.tm_isdst = /* auto-detect DST from date/time in tm */ -1;
      epoch_ms = mktime(&tm) * 1000;

      // Check for optional milliseconds.
      const char* ms_end(ms_start);
      if (*ms_start == '.') {
        while (std::isdigit(*++ms_end)) {
        }
        if ((ms_end - ms_start) == 4) {
          try {
            epoch_ms += std::stoull(ms_start + 1);
          } catch (...) {
            return failed_conversion();
          }
        }
      }
      // Treat unparsed trailing characters as an error.
      if (*ms_end != '\0') {
        return failed_conversion();
      }
    } else {
      // Milliseconds since the unix epoch
      try {
        epoch_ms = stoll(tstr);
      } catch (...) {
        return failed_conversion();
      }
    }
    result = OutTimestamp<Duration>(std::chrono::milliseconds(epoch_ms));
    return true;
  }

 private:
  // Common operator= implementation for assignment from Timestamp or
  // time_point that shares the same clock.
  OutTimestamp<Duration> operator=(const Duration& rhs) {
    duration_.store(rhs, std::memory_order_relaxed);
    return OutTimestamp<Duration>(rhs);
  }

  // Convert internal to external duration representations.
  // Used to import durations in copy/assignment operators.
  template <typename D1, typename D2>
  static D1 to(D2 d) {
    return std::chrono::duration_cast<D1>(d);
  }

  DurationHolder<Duration> duration_;
};

// Timestamp +/- std::chrono::duration.
#define MATH_OP(op)                                                            \
  template <class C, template <typename> class H, class D, class R2, class P2> \
  Timestamp<C,                                                                 \
            detail::Holder,                                                    \
            typename std::common_type<D, std::chrono::duration<R2, P2>>::type> \
  operator op(                                                                 \
      const Timestamp<C, H, D>& ts, const std::chrono::duration<R2, P2>& d) {  \
    return ts.timePoint() op d;                                                \
  }
MATH_OP(+)
MATH_OP(-)
#undef MATH_OP

// Like std::chrono::time_point, promote std::chrono::duration + Timestamp
// to a Timestamp.
template <class R1, class P1, class C, template <typename> class H, class D>
Timestamp<C,
          detail::Holder,
          typename std::common_type<std::chrono::duration<R1, P1>, D>::type>
operator+(const std::chrono::duration<R1, P1>& d,
          const Timestamp<C, H, D>& ts) {
  return d + ts.timePoint();
}

// Subtraction of two Timestamps or a Timestamp and a std::chrono::time_point.
template <class C,
          template <typename> class H1,
          class D1,
          template <typename> class H2,
          class D2>
typename std::common_type<D1, D2>::type
operator-(const Timestamp<C, H1, D1>& ts1, const Timestamp<C, H2, D2>& ts2) {
  return ts1.timePoint() - ts2.timePoint();
}
template <class C, template <typename> class H1, class D1, class D2>
typename std::common_type<D1, D2>::type
operator-(const Timestamp<C, H1, D1>& ts1,
          const std::chrono::time_point<C, D2>& tp2) {
  return ts1.timePoint() - tp2;
}
template <class C, class D1, class D2, template <typename> class H2>
typename std::common_type<D1, D2>::type
operator-(const std::chrono::time_point<C, D1>& tp1,
          const Timestamp<C, H2, D2>& ts2) {
  return tp1 - ts2.timePoint();
}

// Timestamp </>/<=/>=/==/!= Timestamp or std::chrono::time_point using the
// same clock.
#define COMP_OP(op)                                                       \
  template <class C,                                                      \
            template <typename> class H1,                                 \
            class D1,                                                     \
            template <typename> class H2,                                 \
            class D2>                                                     \
  bool operator op(                                                       \
      const Timestamp<C, H1, D1>& lhs, const Timestamp<C, H2, D2>& rhs) { \
    return lhs.timePoint() op rhs.timePoint();                            \
  }                                                                       \
  template <class C, template <typename> class H1, class D1, class D2>    \
  bool operator op(const Timestamp<C, H1, D1>& lhs,                       \
                   const std::chrono::time_point<C, D2>& rhs) {           \
    return lhs.timePoint() op rhs;                                        \
  }
COMP_OP(<)
COMP_OP(>)
COMP_OP(<=)
COMP_OP(>=)
COMP_OP(==)
COMP_OP(!=)
#undef COMP_OP

// Timestamp based on the wall clock.
using SystemTimestamp = Timestamp<std::chrono::system_clock, detail::Holder>;
using AtomicSystemTimestamp =
    Timestamp<std::chrono::system_clock, detail::AtomicHolder>;

// Timestamp based on a monotonic clock.
using SteadyTimestamp = Timestamp<std::chrono::steady_clock, detail::Holder>;
using AtomicSteadyTimestamp =
    Timestamp<std::chrono::steady_clock, detail::AtomicHolder>;

// RecordTimestamps are tied to the system clock,
// but only maintain millisecond resolution.
using RecordTimestamp = Timestamp<std::chrono::system_clock,
                                  detail::Holder,
                                  std::chrono::milliseconds>;
using AtomicRecordTimestamp = Timestamp<std::chrono::system_clock,
                                        detail::AtomicHolder,
                                        std::chrono::milliseconds>;

template <typename TS>
using TimeIntervals =
    boost::icl::interval_set<TS, std::less, boost::icl::closed_interval<TS>>;
template <typename TS>
using TimeInterval = typename TimeIntervals<TS>::interval_type;
using RecordTimeIntervals = TimeIntervals<RecordTimestamp>;
using RecordTimeInterval = TimeInterval<RecordTimestamp>;
enum class TimeIntervalOp { ADD, REMOVE };

inline RecordTimeInterval allRecordTimeInterval() {
  return RecordTimeInterval(RecordTimestamp::min(), RecordTimestamp::max());
}

/**
 * Converts a steady clock time point to a system timestamps
 */
SystemTimestamp
toSystemTimestamp(const std::chrono::steady_clock::time_point& time_point);

/**
 * Formats timestamp in milliseconds since the system clock epoch.
 * Output example: "2015-12-31 23:59:59.999".
 */
std::string format_time_impl(std::chrono::milliseconds timestamp);

/**
 * DEPRECATED
 * Accept timestamp as a duration.
 */
template <typename R, typename P>
inline std::string format_time(std::chrono::duration<R, P> duration) {
  using type = decltype(duration);
  if (duration == type::min()) {
    return "-inf";
  } else if (duration == type::max()) {
    return "+inf";
  } else if (duration.count() == 0) {
    return "0";
  }
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  return format_time_impl(ms);
}

/**
 * Accept any Timestamp or time_point derived from the system clock.
 */
template <
    typename TP,
    typename std::enable_if<
        std::is_same<typename TP::clock, std::chrono::system_clock>::value,
        int>::type = 0>
inline std::string format_time(TP tp) {
  return format_time(tp.time_since_epoch());
}

/**
 * Subtracts ts from the current steady time and outputs
 * a string in the format of "hours:mins:sec.ms".
 */
std::string format_time_since(SteadyTimestamp ts);

template <typename Clock,
          template <typename> class DurationHolder,
          typename Duration>
template <typename Clock2>
typename std::enable_if<std::is_same<Clock2, std::chrono::system_clock>::value,
                        std::string>::type
Timestamp<Clock, DurationHolder, Duration>::toString() const {
  return format_time(duration_.load());
}

template <typename Clock,
          template <typename> class DurationHolder,
          typename Duration>
template <typename Clock2>
typename std::enable_if<
    std::is_same<Clock2, std::chrono::steady_clock>::value,
    Timestamp<std::chrono::system_clock, DurationHolder, Duration>>::type
Timestamp<Clock, DurationHolder, Duration>::approximateSystemTimestamp() const {
  auto timestamp =
      Timestamp<std::chrono::system_clock, DurationHolder, Duration>::now();
  timestamp -= (std::chrono::steady_clock::now() - timePoint());
  return timestamp;
}

/**
 * Required by boost::icl's interval ostream operator<<.
 */
template <typename Clock,
          template <typename> class DurationHolder,
          typename Duration>
std::ostream& operator<<(std::ostream& os,
                         const Timestamp<Clock, DurationHolder, Duration>& ts) {
  os << ts.toString();
  return os;
}

/**
 * Overload the toString() function for steady timestamps.
 * Format: "hours:mins:sec.ms ago".
 */
std::string toString(const SteadyTimestamp& t);

/* Format like std associative containers. */
template <typename TS>
std::string toString(const TimeIntervals<TS>& ranges) {
  return "{" + rangeToString(ranges.begin(), ranges.end()) + "}";
}

}} // namespace facebook::logdevice

namespace folly {
/**
 * folly::format support for Timestamps.
 */
template <typename C, template <typename> class H, typename D>
class FormatValue<facebook::logdevice::Timestamp<C, H, D>> {
 public:
  explicit FormatValue(const facebook::logdevice::Timestamp<C, H, D>& val)
      : val_(val) {}

  template <class FormatCallback>
  void format(FormatArg& arg, FormatCallback& cb) const {
    FormatValue<std::string>(val_.toString()).format(arg, cb);
  }

 private:
  const facebook::logdevice::Timestamp<C, H, D>& val_;
};
} // namespace folly
