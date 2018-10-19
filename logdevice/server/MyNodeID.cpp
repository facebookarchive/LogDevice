/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "MyNodeID.h"

#include <cerrno>
#include <cstring>
#include <ifaddrs.h>

#include <folly/ScopeGuard.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

int MyNodeID::calculateFromTcpPort(const ServerConfig& config, NodeID& out) {
  // Ask the kernel for a list of all network interfaces of the host we are
  // running on.
  struct ifaddrs* ifaddr;
  if (getifaddrs(&ifaddr) != 0) {
    ld_error("getifaddrs() failed. errno=%d (%s)", errno, strerror(errno));
    return -1;
  }

  SCOPE_EXIT {
    freeifaddrs(ifaddr);
  };

  // Now compare each returned address to all hosts in the config.
  for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
    // tun interface address can be null
    if (!ifa->ifa_addr) {
      continue;
    }
    int family = ifa->ifa_addr->sa_family;

    // Only interested in IP addresses
    if (family != AF_INET && family != AF_INET6) {
      continue;
    }

    Sockaddr my_addr(ifa->ifa_addr);
    my_addr.setPort(my_port_);

    for (const auto& it : config.getNodes()) {
      if (it.second.address == my_addr) {
        out = NodeID(it.first, it.second.generation);
        return 0;
      }
    }
  }

  ld_error("no local IP address matches any config entry");
  out = NodeID();
  return -1;
}

int MyNodeID::calculateFromUnixSocket(const ServerConfig& config, NodeID& out) {
  for (const auto& it : config.getNodes()) {
    if (it.second.address.isUnixAddress() &&
        it.second.address.getPath() == unix_socket_) {
      out = NodeID(it.first, it.second.generation);
      return 0;
    }
  }

  ld_error("Could not find unix socket path %s in any config entry",
           unix_socket_.c_str());
  out = NodeID();
  return -1;
}

int MyNodeID::calculate(const ServerConfig& config, NodeID& out) {
  if (my_port_ != -1) {
    return calculateFromTcpPort(config, out);
  } else {
    return calculateFromUnixSocket(config, out);
  }
}

}} // namespace facebook::logdevice
