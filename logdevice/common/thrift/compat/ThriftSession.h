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
#include "logdevice/common/TimerInterface.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/ApiModel_types.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/thrift/compat/ThriftMessageSerializer.h"

namespace apache { namespace thrift {
class ClientReceiveState;
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

class HELLO_Message;
class NetworkDependencies;

namespace thrift {
class LogDeviceAPIAsyncClient;
} // namespace thrift

/**
 * Logical session implemented on top of Thrift. Each session has a defined
 * lifecycle starting from handshake procedure and lives until either one of
 * peer decides to close it or connection breaks.
 * All request sent through single session will end up on same Worker on the
 * other side.
 */
class ThriftSession {
 public:
  /**
   * Current state of the session
   */
  enum class State {
    NEW,         // Session created, not handshaken yet
    HANDSHAKING, // Handshake started but has not completed yet
    ESTABLISHED, // Handshake has completed successfully
    CLOSED       // Session has been closed and awaiting cleanup
  };

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

  State getState() const {
    return state_;
  }

  bool isClosed() const {
    return state_ == State::CLOSED;
  }

  /**
   * Forcible closes the session w/o waiting for complition of inflight
   * requests. This will trigger all on close callbacks to be executed.
   */
  virtual void close(Status reason) = 0;

  /**
   * Updates the connection for the session.
   */
  void setInfo(ConnectionInfo&& info);

  /**
   * Add a row to `table` with information about this session.
   * @see logdevice/common/AdminCommandTable.h
   */
  void fillDebugInfo(InfoSocketsTable&) const;

  // Called when a new message has been received on session
  void onMessage(thrift::Message&&);

 protected:
  // Client/Server specific processing of incoming handshake messages
  // Returns true iff message is successfully processed, otherwise returns false
  // and closes the session
  virtual bool handleHandshakeMessage(Message&&) = 0;

  ConnectionInfo info_;
  NetworkDependencies& deps_;
  // A numan-readable string like
  // "C22566784 ([abcd:1234:5678:90ef:1111:2222:3333:4444]:41406)"
  const std::string description_;
  std::unique_ptr<ThriftMessageSerializer> serializer_;
  State state_ = State::NEW;

  // Shortcut for accessing settings
  const Settings& settings() const;
  // Returns version of serilazation protocol agreed on connection. If handshake
  // has not completed yet returns default version suitable for handshake
  // messages' serder.
  uint16_t getProtocolVersion() const;
  // Ensures the protocol version agreed is sane and does not break our
  // invariants. Returns true iff version is acceptable.
  bool checkProtocol() const;
  // Serializes given message to Thrift with respect to protocol version agreed
  // on the session. Returns nullptr and sets err if serialization fails.
  std::unique_ptr<thrift::Message> serialize(const Message&);
  // Deserializes given message to Thrift with respect to protocol version
  // agreed on the session. Returns nullptr and sets err if deserialization
  // fails.
  std::unique_ptr<Message> deserialize(thrift::Message&&);
};

/**
 * Implementation of Session interface for outgoing ("server") connections.
 */
class ServerSession : public ThriftSession {
 public:
  ServerSession(ConnectionInfo&& info,
                NetworkDependencies& deps,
                std::unique_ptr<thrift::LogDeviceAPIAsyncClient> client);

  void close(Status reason) override;

  NodeID peerNodeID() {
    return info_.peer_name.asNodeID();
  }

  /**
   * Initiates connection and handshake procedure. Note that due to connection
   * pooling on Thrift level the connection may exists by this moment, in this
   * case we will run handshake procedure on existsing connection.
   *
   * @return 0 if connection procedure initiated successfully, -1 otherwise. In
   *         case of err the global err variable will set into error code.
   */
  int connect();

  // Called when stream closed with error
  void onSessionError(folly::exception_wrapper&&);
  // Called when stream closed gracefully by the peer
  void onSessionComplete();

 protected:
  bool handleHandshakeMessage(Message&&) override;

 private:
  std::unique_ptr<thrift::LogDeviceAPIAsyncClient> client_;
  std::unique_ptr<TimerInterface> handshake_timer_;
  // Subscription on incoming messages from peer
  using Subscription =
      apache::thrift::ClientBufferedStream<thrift::Message>::Subscription;
  std::unique_ptr<Subscription> subscription_;
  // Called if attempt to send handshake message fails
  void onHandshakeError(folly::exception_wrapper&&);
  // Called when RPC request for new session completes successfully
  void onHandshakeReply(apache::thrift::ClientReceiveState&&);
  // Called if handshake fails to complete before timeout expires
  void onHandshakeTimeout();
  // Creates a subscription for messages coming through Thrift stream
  void registerStream(apache::thrift::ClientBufferedStream<thrift::Message>&&);

  friend class ThriftSenderTest;
};

/**
 * Implementation of Session interface for incoming ("client") connections.
 * Connection from other nodes are also treated as client if they are initiated
 * by the peer.
 */
class ClientSession : public ThriftSession {
 public:
  ClientSession(ConnectionInfo&& info, NetworkDependencies& deps);

  void close(Status reason) override;

  ClientID peerClientID() {
    return info_.peer_name.asClientID();
  }
};

}} // namespace facebook::logdevice
