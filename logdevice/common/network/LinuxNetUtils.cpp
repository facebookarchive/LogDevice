/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/LinuxNetUtils.h"

#include <cstring>
#ifdef __linux__
#include <linux/tcp.h>
#endif
#include <netinet/in.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {
int LinuxNetUtils::getTCPInfo(TCPInfo* info, int fd) {
#ifdef __linux__
  struct tcp_info tcp_i {};

  socklen_t optlen = sizeof(tcp_i);
  // Cannot include netinet/tcp.h as it has conflicting definitions with
  // linux/tcp.h
  int rv = getsockopt(fd, 6 /* SOL_TCP */, TCP_INFO, &tcp_i, &optlen);

  if (rv != 0) {
    err = E::INTERNAL;
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "getsockopt failed with error: %s.",
                      std::strerror(errno));
    return -1;
  }
  info->app_limited = tcp_i.tcpi_delivery_rate_app_limited;
  info->busy_time = std::chrono::microseconds(tcp_i.tcpi_busy_time);
  info->rwnd_limited_time = std::chrono::microseconds(tcp_i.tcpi_rwnd_limited);
  info->sndbuf_limited_time =
      std::chrono::microseconds(tcp_i.tcpi_sndbuf_limited);
  return 0;
#else
  err = E::NOTSUPPORTED;
  return -1;
#endif
}
}} // namespace facebook::logdevice
