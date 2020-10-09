/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <folly/ExceptionWrapper.h>
#include <thrift/lib/cpp2/async/ClientBufferedStream.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ConnectionInfo.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/settings/Settings.h"

namespace apache { namespace thrift {
class ClientReceiveState;
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

class NetworkDependencies;

/**
 * Logical session implemented on top of Thrift. Each session has a defined
 * lifecycle starting from handshake procedure and lives until either one of
 * peer decides to close it or connection breaks.
 * All request sent through single session will end up on same Worker on the
 * other side.
 */
class ThriftSession {
 public:
  ThriftSession(ConnectionInfo&& info, NetworkDependencies& deps);

  ThriftSession(const ThriftSession&) = delete;
  ThriftSession(ThriftSession&&) = delete;
  ThriftSession& operator=(const ThriftSession&) = delete;
  ThriftSession& operator=(ThriftSession&&) = delete;

  virtual ~ThriftSession() = default;

  const Address& peer() const {
    return info_.peer_name;
  }

  /**
   * Get connection info describing this session.
   */
  const ConnectionInfo& getInfo() const {
    return info_;
  }

  /**
   * Updates the connection for the session.
   */
  void setInfo(ConnectionInfo&& info);

  /**
   * Add a row to `table` with information about this session.
   * @see logdevice/common/AdminCommandTable.h
   */
  void fillDebugInfo(InfoSocketsTable&) const;

 protected:
  ConnectionInfo info_;
  NetworkDependencies& deps_;
  // A numan-readable string like
  // "C22566784 ([abcd:1234:5678:90ef:1111:2222:3333:4444]:41406)"
  const std::string description_;
};

/**
 * Implementation of Session interface for outgoing ("server") connections.
 */
class ServerSession : public ThriftSession {
 public:
  ServerSession(ConnectionInfo&& info, NetworkDependencies& deps);

  NodeID peerNodeID() {
    return info_.peer_name.asNodeID();
  }
};

/**
 * Implementation of Session interface for incoming ("client") connections.
 * Connection from other nodes are also treated as client if they are initiated
 * by the peer.
 */
class ClientSession : public ThriftSession {
 public:
  ClientSession(ConnectionInfo&& info, NetworkDependencies& deps);

  ClientID peerClientID() {
    return info_.peer_name.asClientID();
  }
};

}} // namespace facebook::logdevice
