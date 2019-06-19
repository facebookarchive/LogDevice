/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/MyNodeIDFinder.h"

#include <cerrno>
#include <cstring>
#include <ifaddrs.h>

#include <folly/ScopeGuard.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/* static */ std::unique_ptr<NodeIDMatcher>
NodeIDMatcher::byName(const std::string& name) {
  return std::make_unique<NodeIDMatcher>(name);
}

/* static */ std::unique_ptr<NodeIDMatcher> NodeIDMatcher::byTCPPort(int port) {
  // Ask the kernel for a list of all network interfaces of the host we are
  // running on.
  struct ifaddrs* ifaddr;
  if (getifaddrs(&ifaddr) != 0) {
    ld_error("getifaddrs() failed. errno=%d (%s)", errno, strerror(errno));
    return nullptr;
  }

  SCOPE_EXIT {
    freeifaddrs(ifaddr);
  };

  std::vector<Sockaddr> addresses;

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
    my_addr.setPort(port);

    if (!my_addr.valid()) {
      continue;
    }

    addresses.push_back(std::move(my_addr));
  }

  return std::make_unique<NodeIDMatcher>(std::move(addresses));
}

/* static */ std::unique_ptr<NodeIDMatcher>
NodeIDMatcher::byUnixSocket(const std::string& address) {
  Sockaddr my_addr{address};
  if (!my_addr.valid()) {
    return nullptr;
  }
  return std::make_unique<NodeIDMatcher>(std::vector<Sockaddr>{my_addr});
}

bool NodeIDMatcher::match(
    const configuration::nodes::NodeServiceDiscovery& sd) const {
  switch (type_) {
    case Type::NAME:
      return matchByName(sd, std::get<0>(id_));
    case Type::ADDRESS:
      return matchByAddresses(sd, std::get<1>(id_));
  }
  ld_check(false);
  return false;
}

std::string NodeIDMatcher::toString() const {
  switch (type_) {
    case Type::NAME:
      return std::get<0>(id_);
    case Type::ADDRESS:
      std::vector<std::string> address_strings;
      for (const auto& addr : std::get<1>(id_)) {
        address_strings.push_back(addr.toString());
      }
      return folly::sformat("[{}]", folly::join(", ", address_strings));
  }
  ld_check(false);
  return "";
}

bool NodeIDMatcher::matchByAddresses(
    const configuration::nodes::NodeServiceDiscovery& sd,
    const std::vector<Sockaddr>& addresses) const {
  for (const auto& addr : addresses) {
    if (sd.address == addr) {
      return true;
    }
  }
  return false;
}

bool NodeIDMatcher::matchByName(
    const configuration::nodes::NodeServiceDiscovery& sd,
    const std::string& name) const {
  return sd.name == name;
}

folly::Optional<NodeID> MyNodeIDFinder::calculate(
    const configuration::nodes::NodesConfiguration& config) {
  const auto& svc_sd = config.getServiceDiscovery();
  ld_check(id_matcher_);
  ld_check(svc_sd);
  for (const auto& sd : *svc_sd) {
    if (id_matcher_->match(sd.second)) {
      return config.getNodeID(sd.first);
    }
  }
  ld_error("Failed finding my NodeID in the configuration searching by the ID "
           "matcher: %s",
           id_matcher_->toString().c_str());
  return folly::none;
}

}} // namespace facebook::logdevice
