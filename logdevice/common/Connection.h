/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include "folly/io/async/Request.h"
#include "logdevice/common/Address.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/Socket.h"

namespace facebook { namespace logdevice {

class Worker;

/**
 * this will we a wrapper around our socket which knows about protocol and
 * serialization
 */
class Connection : public Socket {
 public:
  /**
   * Constructs a new Connection, to be connected to a LogDevice
   * server. The calling thread must be a Worker thread.
   *
   * @param server_name     id of server to connect to
   * @param type            type of socket
   * @param flow_group      traffic shaping state shared between sockets
   *                        with the same bandwidth constraints.
   *
   * @return  on success, a new fully constructed Connection is returned. It is
   *          expected that the Connection will be registered with the Worker's
   *          Sender under server_name. On failure throws ConstructorFailed and
   *          sets err to:
   *
   *     INVALID_THREAD  current thread is not running a Worker (debug build
   *                     asserts)
   *     NOTINCONFIG     server_name does not appear in cluster config
   *     INTERNAL        failed to initialize a libevent timer (unlikely)
   */
  explicit Connection(NodeID server_name,
                      SocketType type,
                      ConnectionType conntype,
                      FlowGroup& flow_group);

  /**
   * Used for tests.
   */
  Connection(NodeID server_name,
             SocketType type,
             ConnectionType conntype,
             FlowGroup& flow_group,
             std::unique_ptr<SocketDependencies> deps);

  /**
   * Constructs a new Connection from a TCP socket fd that was returned by
   * accept(). The thread must run a Worker. On success the socket is emplaced
   * on this Sender's .client_sockets_ map with client_name as key.
   *
   * @param fd        fd of the accepted socket. The caller passes
   *                  responsibility for closing fd to the constructor.
   * @param client_name local identifier assigned to this passively accepted
   *                    connection (aka "client address")
   * @param client_addr sockaddr we got from accept() for this client connection
   * @param conn_token  used to keep track of all accepted connections
   * @param type        type of socket
   * @param flow_group  traffic shaping state shared between sockets
   *                    with the same bandwidth constraints.
   *
   * @return  on success, a new fully constructed Connection is returned. On
   *          failure throws ConstructorFailed and sets err to:
   *
   *     INVALID_THREAD  current thread is not running a Worker (debug build
   *                     asserts)
   *     NOMEM           a libevent function could not allocate memory
   *     INTERNAL        failed to set fd non-blocking (unlikely) or failed to
   *                     initialize a libevent timer (unlikely).
   */
  Connection(int fd,
             ClientID client_name,
             const Sockaddr& client_addr,
             ResourceBudget::Token conn_token,
             SocketType type,
             ConnectionType conntype,
             FlowGroup& flow_group);

  /**
   * Used for tests.
   */
  Connection(int fd,
             ClientID client_name,
             const Sockaddr& client_addr,
             ResourceBudget::Token conn_token,
             SocketType type,
             ConnectionType conntype,
             FlowGroup& flow_group,
             std::unique_ptr<SocketDependencies> deps);

  /**
   * Disconnects, deletes the underlying bufferevent, and closes the TCP socket.
   */
  ~Connection() override;

  Connection(const Connection&) = delete;
  Connection(Connection&&) = delete;
  Connection& operator=(const Connection&) = delete;
  Connection& operator=(Connection&&) = delete;
  void close(Status reason) override;

 protected:
  void onConnected() override;
  int onReceived(ProtocolHeader ph, struct evbuffer* inbuf) override;
  /**
   * Called when connection timeout occurs. Either we could not establish the
   * TCP connection after multiple retries or the LD handshake did not complete
   * in time.
   */
  void onConnectTimeout() override;

  /**
   * Called when LD handshake doesn't complete in the allottted time.
   */
  void onHandshakeTimeout() override;

  /**
   * Called when the TCP connection could not be established in time.
   * If n_retries_left_ is positive, will try to connect again.
   */
  void onConnectAttemptTimeout() override;

  void onSent(std::unique_ptr<Envelope>,
              Status,
              Message::CompletionMethod =
                  Message::CompletionMethod::IMMEDIATE) override;

  void onError(short direction, int socket_errno) override;

  void onPeerClosed() override;

  void onBytesPassedToTCP(size_t nbytes_drained) override;

 private:
  void initializeContext();
  bool setWorkerContext();
  Worker* const worker_{nullptr};
};

}} // namespace facebook::logdevice
