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

// Explicit instantiations.
template class Timestamp<std::chrono::system_clock, detail::Holder>;
template class Timestamp<std::chrono::system_clock, detail::AtomicHolder>;
template class Timestamp<std::chrono::steady_clock, detail::Holder>;
template class Timestamp<std::chrono::steady_clock, detail::AtomicHolder>;
template class Timestamp<std::chrono::steady_clock,
                         detail::Holder,
                         std::chrono::milliseconds>;
template class Timestamp<std::chrono::system_clock,
                         detail::Holder,
                         std::chrono::milliseconds>;
template class Timestamp<std::chrono::system_clock,
                         detail::AtomicHolder,
                         std::chrono::milliseconds>;
template std::chrono::steady_clock::duration
steady_to_system_timestamp_approximate<std::chrono::steady_clock::duration>(
    std::chrono::steady_clock::duration steady_timestamp);
template std::chrono::milliseconds
steady_to_system_timestamp_approximate<std::chrono::milliseconds>(
    std::chrono::milliseconds steady_timestamp);

}} // namespace facebook::logdevice
