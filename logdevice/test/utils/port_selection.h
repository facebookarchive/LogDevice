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

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {
namespace detail {

/**
 * RAII-style container for a socket listening on a port.  Closes socket
 * (releasing the port) on destruction.
 */
struct PortOwner : boost::noncopyable {
  PortOwner(int port, int fd) : port(port), fd(fd) {}
  ~PortOwner() {
    int rv = close(fd);
    ld_check(rv == 0);
  }
  int port, fd;
};

using PortOwnerPtrTuple = std::tuple<std::unique_ptr<PortOwner>,  // data
                                     std::unique_ptr<PortOwner>,  // command
                                     std::unique_ptr<PortOwner>>; // admin

/**
 * Tries to claim `npairs` tuples of ports on localhost by opening listening
 * sockets on them.
 *
 * @return 0 on success, -1 on failure
 */
int find_free_port_set(int npairs, std::vector<PortOwnerPtrTuple>& ports_out);

/** Claim one port, bind and listen to it, then construct corresponding
 * PortOwner.
 * returns nullptr if failed
 */
std::unique_ptr<PortOwner> claim_port(int port);

}}}} // namespace facebook::logdevice::IntegrationTestUtils::detail
