/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/SocketTypes.h"

namespace facebook { namespace logdevice {

const char* socketTypeToString(SocketType sock_type) {
  switch (sock_type) {
    case SocketType::DATA:
      return "DATA";
    case SocketType::GOSSIP:
      return "GOSSIP";
  }
  return "";
}

const char* connectionTypeToString(ConnectionType conn_type) {
  switch (conn_type) {
    case ConnectionType::NONE:
      return "NONE";
    case ConnectionType::PLAIN:
      return "PLAIN";
    case ConnectionType::SSL:
      return "SSL";
  }
  return "";
}

const char* peerTypeToString(PeerType peer_type) {
  switch (peer_type) {
    case PeerType::CLIENT:
      return "CLIENT";
    case PeerType::NODE:
      return "NODE";
    default:
      return "";
  }
  return "";
}

const char* socketDrainStatusToString(SocketDrainStatusType type) {
  switch (type) {
    case SocketDrainStatusType::UNKNOWN:
      return "UNKNOWN";
    case SocketDrainStatusType::ACTIVE:
      return "ACTIVE";
    case SocketDrainStatusType::STALLED:
      return "STALLED";
    case SocketDrainStatusType::NET_SLOW:
      return "NET_SLOW";
    case SocketDrainStatusType::RECV_SLOW:
      return "RECV_SLOW";
    case SocketDrainStatusType::IDLE:
      return "IDLE";
  }
  return "";
}
}} // namespace facebook::logdevice
