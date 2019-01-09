/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Sockaddr.h"

#include <system_error>

#include <folly/Conv.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

Sockaddr Sockaddr::INVALID;

Sockaddr::Sockaddr(const std::string& ip, in_port_t port) {
  try {
    addr_.setFromIpPort(ip.c_str(), port);
  } catch (std::invalid_argument& e) {
    err = E::INVALID_PARAM;
    throw ConstructorFailed();
  } catch (std::system_error& e) {
    err = E::FAILED;
    throw ConstructorFailed();
  }

  if (getAddress().isLinkLocalBroadcast() || getAddress().isMulticast()) {
    err = E::INVALID_IP;
    throw ConstructorFailed();
  }
}

Sockaddr::Sockaddr(const std::string& ip, const std::string& port)
    : Sockaddr(ip, folly::to<in_port_t>(port)) {}

Sockaddr::Sockaddr(const std::string& path) {
  try {
    addr_.setFromPath(path);
  } catch (std::invalid_argument& e) {
    err = E::INVALID_PARAM;
    throw ConstructorFailed();
  } catch (std::system_error& e) {
    err = E::FAILED;
    throw ConstructorFailed();
  }
}

Sockaddr::Sockaddr(const struct sockaddr* sa) {
  try {
    addr_.setFromSockaddr(sa);
  } catch (std::invalid_argument& e) {
    err = E::INVALID_PARAM;
    throw ConstructorFailed();
  } catch (std::system_error& e) {
    err = E::FAILED;
    throw ConstructorFailed();
  }
}

Sockaddr::Sockaddr(const struct sockaddr* sa, int len) {
  try {
    addr_.setFromSockaddr(sa, len);
  } catch (std::invalid_argument& e) {
    err = E::INVALID_PARAM;
    throw ConstructorFailed();
  } catch (std::system_error& e) {
    err = E::FAILED;
    throw ConstructorFailed();
  }
}

in_port_t Sockaddr::port() const {
  try {
    return addr_.getPort();
  } catch (std::invalid_argument& e) {
    ld_check(false);
    return 0;
  }
}

void Sockaddr::setPort(in_port_t port) {
  try {
    addr_.setPort(port);
  } catch (std::invalid_argument& e) {
    ld_check(false);
  }
}

int Sockaddr::toStructSockaddr(struct sockaddr_storage* sockaddr_out) const {
  if (!valid()) {
    // toStructSockaddr() called on invalid Sockaddr
    ld_check(false);
    return -1;
  }

  return addr_.getAddress(sockaddr_out);
}

folly::IPAddress Sockaddr::getAddress() const {
  try {
    return addr_.getIPAddress();
  } catch (std::invalid_argument& e) {
    ld_check(false);
    return folly::IPAddress();
  }
}

std::string Sockaddr::getPath() const {
  try {
    return addr_.getPath();
  } catch (std::invalid_argument& e) {
    ld_check(false);
    return std::string();
  }
}

std::string Sockaddr::toStringImpl(bool with_brackets, bool with_port) const {
  if (!valid()) {
    return "INVALID";
  }
  auto port_maybe = [&]() {
    if (!with_port) {
      return std::string();
    }
    return ":" + std::to_string(addr_.getPort());
  };

  switch (family()) {
    case AF_INET6:
      if (with_brackets) {
        return '[' + addr_.getAddressStr() + "]" + port_maybe();
      }
    case AF_INET:
      return addr_.getAddressStr() + port_maybe();
    case AF_UNIX: {
      std::string socket_path = addr_.getPath();
      if (socket_path.empty()) {
        // Folly::SocketAddress will return an empty string if the unix socket
        // is anonymous. This is true in the case of a client socket connected
        // through a unix-socket.
        socket_path = "localhost";
      }
      return socket_path;
    }
    default:
      ld_check(false);
      return "<unknown address family>";
  }
}

bool operator==(const Sockaddr& a1, const Sockaddr& a2) {
  return a1.addr_ == a2.addr_;
}
bool operator!=(const Sockaddr& a1, const Sockaddr& a2) {
  return !(a1 == a2);
}

Sockaddr Sockaddr::withPort(in_port_t new_port) const {
  Sockaddr copy = *this;
  try {
    copy.setPort(new_port);
  } catch (std::invalid_argument& e) {
    ld_check(false);
    return Sockaddr();
  }
  return copy;
}

/*static*/
folly::Optional<Sockaddr> Sockaddr::fromString(const std::string& hostStr) {
  if (hostStr.empty()) {
    return folly::none;
  }

  try {
    if (hostStr[0] == '/') {
      // The address string contains the path for a unix domain socket.
      return Sockaddr(hostStr.c_str());
    }

    // it's an ipv4/ipv6 address
    std::pair<std::string, std::string> ipPortPair = parse_ip_port(hostStr);
    if (ipPortPair.first.empty() || ipPortPair.second.empty()) {
      ld_error("invalid sockaddr string: \"%s\"", hostStr.c_str());
      return folly::none;
    }
    return Sockaddr(ipPortPair.first, ipPortPair.second);
  } catch (const ConstructorFailed&) {
    ld_error("invalid sockaddr string: \"%s\"", hostStr.c_str());
    return folly::none;
  }
}

}} // namespace facebook::logdevice
