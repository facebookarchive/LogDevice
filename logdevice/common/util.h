/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <alloca.h>
#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/icl/interval_set.hpp>
#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/chrono_util.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

/**
 * @file   Miscellaneous utilities.
 */

namespace facebook { namespace logdevice {

// represents an interval lo..hi. Using a C naming convention to clearly
// distinguish this POD from boost::icl::interval.
struct interval_t {
  uint64_t lo;
  uint64_t hi;

  interval_t() = default;
  interval_t(uint64_t lo, uint64_t hi) : lo(lo), hi(hi) {}
};

template <typename Duration>
struct chrono_interval_t {
  typedef Duration duration_t;
  duration_t lo;
  duration_t hi;
};

template <typename Duration>
struct chrono_expbackoff_t {
  typedef Duration duration_t;
  typedef typename Duration::rep multiplier_t;

  /// Binary exponential backoff is the default.
  static constexpr multiplier_t DEFAULT_MULTIPLIER = 2;

  /**
   * Default constructor.
   *
   * Warning: Does not initialize anything.
   */
  chrono_expbackoff_t() noexcept = default;

  /**
   * Conversion constructor. Accepts all duration types that have implicit
   * conversions to Duration.
   */
  template <typename OtherDuration>
  chrono_expbackoff_t(const chrono_expbackoff_t<OtherDuration>& other) noexcept
      : initial_delay(other.initial_delay),
        max_delay(other.max_delay),
        multiplier(other.multiplier) {}

  /**
   * Construct from parts. Accepts all duration types that have implicit
   * conversions to Duration.
   */
  template <typename D1, typename D2>
  chrono_expbackoff_t(D1 initial_delay,
                      D2 max_delay,
                      multiplier_t multiplier = DEFAULT_MULTIPLIER) noexcept
      : initial_delay(initial_delay),
        max_delay(max_delay),
        multiplier(multiplier) {}

  bool operator==(const chrono_expbackoff_t<Duration>& other) {
    return other.initial_delay == initial_delay &&
        other.max_delay == max_delay && other.multiplier == multiplier;
  }
  bool operator!=(const chrono_expbackoff_t<Duration>& other) {
    return !(*this == other);
  }

  duration_t initial_delay;
  duration_t max_delay;
  multiplier_t multiplier;
};

using log_ranges_t = boost::icl::interval_set<
    logid_t::raw_type,
    std::less,
    boost::icl::right_open_interval<logid_t::raw_type, std::less>>;

template <class ForwardIterator>
bool isStrictlyAscending(ForwardIterator first, ForwardIterator last) {
  if (first == last) {
    return true;
  }
  ForwardIterator next = first;
  while (++next != last) {
    if (!(*first < *next)) {
      return false;
    }
    ++first;
  }
  return true;
}

/**
 * Parse a string containing a decimal number optionally followed by one
 * of the letters [KMGT] that denote a multipler. 1K=1000, 1T=1 trillion.
 * The function is intended for parsing limit values in lo..hi interval
 * expressions.
 *
 * @param s the string to parse
 * @param l the resulting limit value (an out parameter)
 * @return  0 on success, -1 if the string is not in a correct format.
 */
int parse_scaled_int(const char* s, uint64_t* l);

/**
 * Parse an interval expression of the form lo(..hi)? where lo and hi have the
 * format described in parse_scaled_int() above.
 *
 * @param s  string to parse
 * @param r  a interval_t object to fill on success
 *
 * @return   0 on success, -1 if the strings is not in a correct format
 */
int parse_interval(const char* s, interval_t* r);

/**
 * Parse a comma-separated list of intervals of the form that parse_interval()
 * accepts.
 *
 * @param s               string to parse
 * @param out_log_ranges  the object to fill on success
 *
 * @return   0 on success, -1 if the strings is not in a correct format
 */
int parse_logid_intervals(const char* s, log_ranges_t* out_log_ranges);

/**
 * Same as parse_logid_intervals() but materializes the ranges into list of
 * log IDs.
 *
 * @param s         string to parse
 * @param out_logs  the object to fill on success
 *
 * @return   0 on success, -1 if the strings is not in a correct format
 */
int parse_logid_intervals(const char* s, std::vector<logid_t>* out_logs_ids);

/**
 * Parse a rate limit of the form <count><suffix>/<duration><unit>, e.g. 10M/1s.
 * Numerator is parsed with parse_scaled_int(), denominator is parsed with
 * parse_chrono_string().
 *
 * @param s  string to parse
 * @param r  object to fill on success
 *
 * @return   0 on success, -1 if the strings is not in a correct format
 */
int parse_rate_limit(const char* s, rate_limit_t* r);

/**
 * Parse an IO priority (argument of set_io_priority_of_this_thread()).
 */
int parse_ioprio(const std::string& val,
                 folly::Optional<std::pair<int, int>>* out_prio);

/**
 * Parse a compaction schedule, which isa list of durations, e.g. "3d,7d".
 *
 * @param val string to parse
 * @param r   vector to fill on success
 * @return    0 on success, -1 if the strings is not in a correct format
 */
int parse_compaction_schedule(
    const std::string& val,
    folly::Optional<std::vector<std::chrono::seconds>>& out);

/**
 * Attempts to parse the input string as "ip:port" and return the two parts.
 * Differentiates between ipv4 and ipv6 addresses but does not fully validate
 * them.  (It is assumed that the ip part will go through a more robust
 * parsing facility such as getaddrinfo().)
 *
 * For ipv4 addresses, this function admits "[0-9.]+";
 * For ipv6 addresses, this function admits "\[[0-9a-fA-F:.]+\]" but strips the
 * brackets.
 *
 * @return pair<ip, host> or a pair of empty strings if there was a parse error
 */
std::pair<std::string, std::string> parse_ip_port(const std::string& host_str);

/**
 * Print n into buf of size _size_. Separate 000 digit groups with commas.
 *
 * @return a pointer into buf on success, nullptr if buf is too small.
 */
const char* commaprint_r(unsigned long n, char* buf, int size);

/**
 * Convert a string into an LSN. recognized formats are
 * - e<epoch>n<esn>
 * - <lsn>
 * - LSN_INVALID
 * - LSN_OLDEST
 * - LSN_MAX
 *
 * return 0 if conversion succeeded, -1 otherwise
 */
int string_to_lsn(std::string in, lsn_t& out);

/**
 * Convert logid into a string in the format M<num> for metadata logs, L<num>
 * for non-metadata logs.
 */
std::string toString(const logid_t);

/**
 * Format a copyset / storage set into string in the format {N1:S5,N2:S8,N3:S3}.
 */
std::string toString(const StorageSet& storage_set);
std::string toString(const ShardID* copyset, size_t size);

/**
 *  Convert KeyType to human-readable string
 */
std::string toString(const KeyType& type);

/**
 * Utility for safely printing a buffer that might not be null terminated
 * and/or might contain binary data.
 */
std::string safe_print(const char* data, size_t buf_size);

/**
 * Formats keys of a map into a delim-separated string.
 */
template <typename M>
std::string map_keys_join(const std::string& delim, const M& map) {
  std::string s;
  bool first = true;
  for (const auto& kv : map) {
    if (first) {
      first = false;
    } else {
      s += delim;
    }
    s += std::to_string(kv.first);
  }
  return s;
}

/**
 * Makes a string safe to print to log: replaces non-printable characters;
 * truncates to max_len of too long, appending "...[424242 bytes]".
 */
std::string sanitize_string(std::string s, int max_len = 1024);

/**
 * Sanitizes text to be displayed in a Markdown table cell or title.
 */
std::string markdown_sanitize(const std::string& str);

/**
 * Returns whether a string has any type of whitespaces in it or not.
 */
inline bool contains_whitespaces(const std::string& s) {
  return s.end() !=
      std::find_if(s.begin(), s.end(), [](int ch) { return std::isspace(ch); });
}

/**
 * Remove whitespaces from the left end of a string
 */
inline void lstrip_string(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
            return !std::isspace(ch);
          }));
}

/**
 * Remove whitespaces from the right end of a string
 */
inline void rstrip_string(std::string& s) {
  s.erase(std::find_if(
              s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

/**
 * Remove whitespaces from both ends of a string
 */
inline void strip_string(std::string& s) {
  lstrip_string(s);
  rstrip_string(s);
}

/**
 * Workaround for almost infinite sleep in std::this_thread::sleep_until().
 * See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=58038
 * Fixed in gcc 4.9.3
 */
template <typename _TimePoint>
inline void sleep_until_safe(const _TimePoint& atime) {
  auto now = _TimePoint::clock::now();
  if (atime > now) {
    /* sleep override */
    std::this_thread::sleep_for(atime - now);
  }
}

/**
 * Simple deleter that can be used in a std::shared_ptr to delete an object
 * that was placement-constructed in a malloc'd buffer.
 */
template <typename T>
struct DestructFreeDeleter {
  void operator()(T* ptr) {
    // No null check needed, guaranteed by standard [util.smartptr.shared.dest]
    ptr->~T();
    std::free(ptr);
  }
};

/**
 * Atomically does the following:  if (f(a, arg)) a = val;
 * and obtains the value of `a' held previously.
 */
template <typename T, typename Atom, typename F>
T atomic_conditional_store(Atom& a, const T val, const T arg, F f) {
  T a_observed = a.load();
  while (f(a_observed, arg) && !a.compare_exchange_strong(a_observed, val)) {
  }
  return a_observed;
}

/**
 * Atomically performs a = max(a, val) and obtains the value held previously.
 */
template <typename T, typename Atom>
T atomic_fetch_max(Atom& a, const T val) {
  return atomic_conditional_store(a, val, val, std::less<T>());
}

/**
 * Atomically performs a = min(a, val) and obtains the value held previously.
 */
template <typename T, typename Atom>
T atomic_fetch_min(Atom& a, const T val) {
  return atomic_conditional_store(a, val, val, std::greater<T>());
}

/**
 * Replaces all unprintable characters (including newlines) in @param
 * str with spaces. Use this function when writing unknown strings
 * into logdeviced error log. It will help us maintain the error log
 * in a relatively regular format that can be parsed by tools.
 *
 * @return  @param str after transformation.
 */
char* error_text_sanitize(char* str);

/**
 * Dump the content of the buf to a string in hex format
 *
 * @param max_output_size  if the full hex string would be longer than that,
 *                         shorten to "123ABC...[45 bytes]"
 */
std::string
hexdump_buf(const void* buf,
            size_t size,
            size_t max_output_size = std::numeric_limits<size_t>::max());

inline std::string
hexdump_buf(const Slice& slice,
            size_t max_output_size = std::numeric_limits<size_t>::max()) {
  return hexdump_buf(slice.data, slice.size, max_output_size);
}

/**
 * A simple wrapper around an object with a TTL assigned.
 */
template <typename T>
class entry_with_ttl {
 public:
  explicit entry_with_ttl(const T& object)
      : object_(object), created_at_(std::chrono::steady_clock::now()) {}

  template <typename U,
            typename =
                typename std::enable_if<std::is_convertible<U, T>::value>::type>
  explicit entry_with_ttl(U&& object)
      : object_(std::move(object)),
        created_at_(std::chrono::steady_clock::now()) {}

  template <typename Duration>
  bool isValid(Duration ttl) const {
    return std::chrono::steady_clock::now() <= created_at_ + ttl;
  }
  const T& get() const {
    return object_;
  }
  T& get() {
    return object_;
  }

 private:
  T object_;
  std::chrono::steady_clock::time_point created_at_;
};

/* Template to downcast pointers/references with dynamic_cast checks */
template <typename ToPtr, typename From>
ToPtr checked_downcast(From* ptr) {
  ld_check(ptr != nullptr);
  ld_assert(dynamic_cast<ToPtr>(ptr) != nullptr);
  return static_cast<ToPtr>(ptr);
}

template <typename ToPtr, typename From>
ToPtr checked_downcast_or_null(From* ptr) {
  return ptr == nullptr ? nullptr : checked_downcast<ToPtr>(ptr);
}

template <typename ToRef, typename From>
ToRef checked_downcast(From& ref) {
  return *checked_downcast<typename std::add_pointer<ToRef>::type>(&ref);
}

template <typename ToUniquePtr, typename From>
ToUniquePtr checked_downcast(std::unique_ptr<From> ptr) {
  return ToUniquePtr(
      checked_downcast<typename ToUniquePtr::pointer>(ptr.release()));
}

template <typename ToSharedPtr, typename From>
ToSharedPtr checked_downcast(const std::shared_ptr<From>& ptr) {
  ld_assert(std::dynamic_pointer_cast<typename ToSharedPtr::element_type>(ptr));
  return std::static_pointer_cast<typename ToSharedPtr::element_type>(ptr);
}

template <typename T>
void set_if_not_null(std::add_pointer_t<folly::remove_cvref_t<T>> output,
                     T&& value) {
  if (output) {
    *output = std::forward<T>(value);
  }
}

template <typename Ptr>
static bool compare_obj_ptrs(Ptr l, Ptr r) {
  if (l == nullptr && r == nullptr) {
    return true;
  }
  if (l == nullptr || r == nullptr) {
    return false;
  }
  return *l == *r;
}

/**
 * Wrap ioprio_set() and ioprio_get() syscalls.
 * @return 0 on success, -1 on error
 */
int set_io_priority_of_this_thread(std::pair<int, int> prio);
int get_io_priority_of_this_thread(std::pair<int, int>* out_prio);

/* Template to remove duplicates from vector of objects */
template <typename T>
void removeDuplicates(std::vector<T>* out_objects) {
  ld_check(out_objects != nullptr);
  std::sort(out_objects->begin(), out_objects->end());
  out_objects->erase(std::unique(out_objects->begin(), out_objects->end()),
                     out_objects->end());
}

// Erases a single element of the vector by swapping it with the last element
// first. Note that this will change the order of the elements in the vector
// unless you are erasing the last element.
template <typename T, typename A>
void erase_from_vector(std::vector<T, A>& v, size_t offset) {
  ld_check(offset < v.size());
  ld_check(v.size() > 0);
  if (offset != v.size() - 1) {
    using std::swap;
    swap(v.back(), v[offset]);
  }
  v.pop_back();
}

// Converts the string to lower case.
// (By applying ::tolower() to each character.)
std::string lowerCase(std::string s);

// Gets the username of the euid (or uid if effective==false) the process is
// running with
std::string get_username(bool effective = true);

template <typename T>
constexpr std::underlying_type_t<T> to_integral(const T value) {
  return static_cast<std::underlying_type_t<T>>(value);
}

// Does initialization common to most logdevice executables:
//  * Call folly::SingletonVault::singleton()->registrationComplete().
//  * Ignore SIGPIPE.
//  * Die when parent process dies: prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0).
// Call it at the beginning of main().
void logdeviceInit();

}} // namespace facebook::logdevice

// See commaprint_r(). Since this version uses alloca() for the result, it
// could be dangerous to use in long-running loops.
#define commaprint(n) \
  facebook::logdevice::commaprint_r((n), (char*)alloca(30), 30)
