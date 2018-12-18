/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <vector>

#include <boost/program_options.hpp>
#include <folly/small_vector.h>

#include "logdevice/common/util.h"

/**
 * @file common validators for settings - validating that a setting value is
 *       within a specified range, or one of a set of allowed values, etc.
 */

namespace facebook { namespace logdevice { namespace setting_validators {

void throw_invalid_value(const char* optname,
                         const char* expected,
                         const std::string& val);

void throw_invalid_value(const char* optname,
                         const char* expected,
                         ssize_t num);

void validate_string_one_of(const char* optname,
                            const std::vector<std::string>& allowed,
                            const std::string& value);

void checksum_bits_notifier(int val);

// The type used to store multiple values for conversions for validators
template <typename To>
using mv_t = folly::small_vector<To, 3>;

// Conversions for validators of setting values
template <typename To, typename From>
mv_t<To> convert(From val) {
  return mv_t<To>(1, static_cast<To>(val));
}

template <typename To, typename SubType, typename Ratio>
mv_t<To> convert(std::chrono::duration<SubType, Ratio> val) {
  return convert<To>(val.count());
}

template <typename To, typename Duration>
mv_t<To> convert(chrono_interval_t<Duration> val) {
  mv_t<To> res = convert<To>(val.lo);
  mv_t<To> res2 = convert<To>(val.hi);
  res.insert(res.end(), res2.begin(), res2.end());
  return res;
}

template <typename To, typename Duration>
mv_t<To> convert(chrono_expbackoff_t<Duration> val) {
  mv_t<To> res = convert<To>(val.initial_delay);
  mv_t<To> res2 = convert<To>(val.max_delay);
  res.insert(res.end(), res2.begin(), res2.end());
  mv_t<To> res3 = convert<To>(val.multiplier);
  res.insert(res.end(), res3.begin(), res3.end());
  return res;
}

template <typename To, typename A, typename B>
mv_t<To> convert(std::pair<A, B> val) {
  mv_t<To> res = convert<To>(val.first);
  mv_t<To> res2 = convert<To>(val.second);
  res.insert(res.end(), res2.begin(), res2.end());
  return res;
}

// Conversions for validators of setting values
template <typename To>
bool p_convert(const std::string& vstr, mv_t<To>& res) {
  if (vstr == "-1") {
    res = mv_t<To>(1, -1);
    return true;
  }
  uint64_t val;
  if (parse_scaled_int(vstr.c_str(), &val) != 0) {
    return false;
  }
  res = mv_t<To>(1, val);
  return true;
}

// The template for validating a setting value
template <typename TargetType>
class parse_value {
 public:
  ssize_t operator()(const char* name, const std::string& value) {
    mv_t<TargetType> vals;
    if (!p_convert<TargetType>(value, vals)) {
      fail(name, value);
    }
    for (auto v : vals) {
      if (!valid_value(v)) {
        auto v_str = std::to_string(v);
        fail(name, v_str);
      }
    }
    return vals.front();
  }

  virtual ~parse_value() {}

 private:
  virtual bool valid_value(TargetType value) = 0;
  virtual const char* value_requirement_name() = 0;
  virtual void fail(const char* name, const std::string& value) {
    throw_invalid_value(name, value_requirement_name(), value);
  }
};

// A validator that throws an exception if the setting value is not positive
template <typename TargetType>
class parse_positive : public parse_value<TargetType> {
 private:
  bool valid_value(TargetType val) override {
    return val > 0;
  }
  const char* value_requirement_name() override {
    return "positive";
  }
};

// A validator that throws an exception if the setting value is negative
template <typename TargetType>
class parse_nonnegative : public parse_value<TargetType> {
 private:
  bool valid_value(TargetType val) override {
    return val >= 0;
  }
  const char* value_requirement_name() override {
    return "non-negative";
  }
};

// A validator that throws an exception if the setting value falls outside the
// range of [min, max]
template <typename TargetType>
class parse_validate_range : public parse_value<TargetType> {
 public:
  parse_validate_range(TargetType min, TargetType max) : min_(min), max_(max) {}

 protected:
  const TargetType min_;
  const TargetType max_;

 private:
  bool valid_value(TargetType val) override {
    return val >= min_ && val <= max_;
  }
  const char* value_requirement_name() override {
    return "validate_range";
  }
  void fail(const char* name, const std::string& /*value*/) override {
    throw boost::program_options::error(
        "Invalid value for --" + std::string(name) + ". Must be between " +
        std::to_string(min_) + " and " + std::to_string(max_));
  }
};

// A validator that throws an exception if the setting value is smaller than
// min
template <typename TargetType>
class parse_validate_lower_bound : public parse_validate_range<TargetType> {
 public:
  explicit parse_validate_lower_bound(TargetType min)
      : parse_validate_range<TargetType>(
            min,
            std::numeric_limits<TargetType>::max()) {}

 private:
  void fail(const char* name, const std::string& /*value*/) override {
    throw boost::program_options::error("Invalid value for --" +
                                        std::string(name) + ". Must be > " +
                                        std::to_string(this->min_));
  }
};

// The template for validating a setting value
template <typename TargetType>
class validate_value {
 public:
  template <typename T>
  void operator()(const char* name, T value) {
    auto vals = convert<TargetType>(value);
    for (auto v : vals) {
      if (!valid_value(v)) {
        fail(name, v);
      }
    }
  }

  virtual ~validate_value() {}

 private:
  virtual bool valid_value(TargetType value) = 0;
  virtual const char* value_requirement_name() = 0;
  virtual void fail(const char* name, TargetType value) {
    throw_invalid_value(name, value_requirement_name(), value);
  }
};

// A validator that throws an exception if the setting value is not positive
template <typename TargetType>
class validate_positive : public validate_value<TargetType> {
 private:
  bool valid_value(TargetType val) override {
    return val > 0;
  }
  const char* value_requirement_name() override {
    return "positive";
  }
};

// A validator that throws an exception if the setting value is negative
template <typename TargetType>
class validate_nonnegative : public validate_value<TargetType> {
 private:
  bool valid_value(TargetType val) override {
    return val >= 0;
  }
  const char* value_requirement_name() override {
    return "non-negative";
  }
};

// A validator that throws an exception if the setting value falls outside the
// range of [min, max]
template <typename TargetType>
class validate_range : public validate_value<TargetType> {
 public:
  validate_range(TargetType min, TargetType max) : min_(min), max_(max) {}

 protected:
  const TargetType min_;
  const TargetType max_;

 private:
  bool valid_value(TargetType val) override {
    return val >= min_ && val <= max_;
  }
  const char* value_requirement_name() override {
    return "validate_range";
  }
  void fail(const char* name, TargetType /*value*/) override {
    throw boost::program_options::error(
        "Invalid value for --" + std::string(name) + ". Must be between " +
        std::to_string(min_) + " and " + std::to_string(max_));
  }
};

// A validator that throws an exception if the setting value is smaller than
// min
template <typename TargetType>
class validate_lower_bound : public validate_range<TargetType> {
 public:
  explicit validate_lower_bound(TargetType min)
      : validate_range<TargetType>(min,
                                   std::numeric_limits<TargetType>::max()) {}

 private:
  void fail(const char* name, TargetType /*value*/) override {
    throw boost::program_options::error("Invalid value for --" +
                                        std::string(name) + ". Must be >" +
                                        std::to_string(this->min_));
  }
};

// A parser that parses a memory budget. The value can expressed in the form of
// a percentage of all the memory available in the system or as an absolute
// value in bytes.
// Example of valid values: "10G", "1073741824", "10K", "5%".
class parse_memory_budget {
 public:
  virtual ~parse_memory_budget() {}
  size_t operator()(const char* name, const std::string& value);

 protected:
  // Can be overridden by tests.
  virtual size_t getAvailableMemory();
};

void validate_unix_socket(const std::string& unix_socket);

void validate_port(int port);

}}} // namespace facebook::logdevice::setting_validators
