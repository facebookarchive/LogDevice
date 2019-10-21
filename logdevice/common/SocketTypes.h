/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice {

enum class SocketType : uint8_t { DATA, GOSSIP };
enum class ConnectionType : uint8_t { NONE, PLAIN, SSL };
enum class PeerType : uint8_t {
  FIRST = 0,
  CLIENT = FIRST,
  NODE,
  NUM_PEER_TYPES
};
enum class SocketDrainStatusType : uint8_t {
  UNKNOWN,
  ACTIVE,
  STALLED,
  NET_SLOW,
  RECV_SLOW,
  IDLE
};

const char* socketTypeToString(SocketType sock_type);
const char* connectionTypeToString(ConnectionType conn_type);
const char* peerTypeToString(PeerType peer_type);
const char* socketDrainStatusToString(SocketDrainStatusType type);

}} // namespace facebook::logdevice
