/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

namespace facebook { namespace logdevice {

/**
 * Various network related linux specific utility functions. This helps in
 * keeping linux related includes to a single file.
 */
struct TCPInfo {
  TCPInfo()
      : app_limited(false),
        busy_time(0),
        rwnd_limited_time(0),
        sndbuf_limited_time(0) {}
  bool app_limited;
  std::chrono::microseconds busy_time;
  std::chrono::microseconds rwnd_limited_time;
  std::chrono::microseconds sndbuf_limited_time;
};
class LinuxNetUtils {
 public:
  int getTCPInfo(TCPInfo* info, int fd);
};

}} // namespace facebook::logdevice
