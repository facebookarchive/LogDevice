/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/Address.h"
#include "logdevice/common/ConnectionKind.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"

namespace facebook { namespace logdevice {

/**
 * Information about connection peer. Some fields are optional because they
 * either may be not known for some types of connections or set during handshake
 * procedure and not known beforehand.
 */
struct ConnectionInfo {
  ConnectionInfo(const Address& name,
                 const Sockaddr& address,
                 SocketType s_type,
                 ConnectionType c_type)
      : peer_name(name),
        peer_address(address),
        socket_type(s_type),
        connection_type(c_type) {}

  /**
   * LogDevice-level address of peer end-point at the other end of the
   * connection.
   */
  Address peer_name;

  /**
   * Physical address of remote peer.
   */
  Sockaddr peer_address;

  /**
   * Purpose of connection: gossip or data.
   */
  SocketType socket_type;

  /**
   * Type of connection: SSL or plain data.
   */
  ConnectionType connection_type;

  /**
   * Protocol version negotiated following handshake, before handshake must be
   * folly::none.
   */
  folly::Optional<uint16_t> protocol;

  /**
   * Produces a numan-readable string like
   * "C22566784 ([abcd:1234:5678:90ef:1111:2222:3333:4444]:41406)"
   */
  std::string describe() {
    auto address_str =
        peer_address.valid() ? peer_address.toString() : std::string("UNKNOWN");
    return peer_name.toString() + "(" + address_str + ")";
  }
};

}} // namespace facebook::logdevice
