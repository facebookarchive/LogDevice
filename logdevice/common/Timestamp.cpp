/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Timestamp.h"

#include <cstdio>
#include <ctime>

namespace facebook { namespace logdevice {

SystemTimestamp
toSystemTimestamp(const std::chrono::steady_clock::time_point& time_point) {
  SystemTimestamp timestamp(std::chrono::system_clock::now());
  timestamp -= (std::chrono::steady_clock::now() - time_point);
  return timestamp;
}

std::string format_time_impl(std::chrono::milliseconds timestamp) {
  const char* const err_str = "error";

  time_t seconds = timestamp.count() / 1000;
  struct tm t;
  auto rv = localtime_r(&seconds, &t);
  if (rv == nullptr) {
    return err_str;
  }

  char buf[40];
  size_t len = strftime(buf, sizeof(buf), "%F %T", &t);
  if (len == 0) {
    return err_str;
  }
  snprintf(buf + len, sizeof(buf) - len, ".%03lu", timestamp.count() % 1000);

  return buf;
}

std::string format_time_since(SteadyTimestamp timestamp) {
  SteadyTimestamp since(SteadyTimestamp::now() - timestamp);
  auto h = since.toHours();
  auto m = since.toMinutes();
  auto s = since.toSeconds();
  auto ms = since.toMilliseconds();
  char buf[40];
  snprintf(buf,
           sizeof(buf),
           "%lu:%02lu:%02lu.%03lu",
           h.count(),
           m.count() % 60,
           s.count() % 60,
           ms.count() % 1000);
  return buf;
}

std::string toString(const SteadyTimestamp& t) {
  return format_time_since(t) + " ago";
}

}} // namespace facebook::logdevice
