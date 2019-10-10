/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unistd.h>
#include <utility>
#include <vector>

#include <boost/noncopyable.hpp>
#include <folly/Optional.h>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {
namespace detail {

/**
 * RAII-style container for a socket listening on a port.  Closes socket
 * (releasing the port) on destruction.
 */
struct PortOwner {
  int port = -1, fd = -1;

  PortOwner() = default;
  PortOwner(int port, int fd) : port(port), fd(fd) {}
  PortOwner(PortOwner&& rhs) noexcept : port(rhs.port), fd(rhs.fd) {
    rhs.release();
  }
  PortOwner& operator=(PortOwner&& rhs) {
    reset();
    port = rhs.port;
    fd = rhs.fd;
    rhs.release();
    return *this;
  }
  ~PortOwner() {
    reset();
  }

  PortOwner(const PortOwner& rhs) = delete;
  PortOwner& operator=(const PortOwner& rhs) = delete;

  bool valid() const {
    return fd != -1;
  }

  void reset() {
    if (!valid()) {
      return;
    }

    int rv = close(fd);
    ld_check(rv == 0);

    port = -1;
    fd = -1;
  }

  void release() {
    port = -1;
    fd = -1;
  }
};

/**
 * Tries to claim `npairs` tuples of ports on localhost by opening listening
 * sockets on them.
 *
 * @return 0 on success, -1 on failure
 */
int find_free_port_set(size_t count, std::vector<PortOwner>& ports_out);

/** Claim one port, bind and listen to it, then construct corresponding
 * PortOwner.
 * returns folly::none if failed
 */
folly::Optional<PortOwner> claim_port(int port);

}}}} // namespace facebook::logdevice::IntegrationTestUtils::detail
