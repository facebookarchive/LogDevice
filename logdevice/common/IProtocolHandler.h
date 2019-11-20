/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/futures/Future.h>

#include "logdevice/include/Err.h"

namespace folly {
class IOBuf;
class AsyncSocketException;
class SocketAddress;
} // namespace folly

namespace facebook { namespace logdevice {
struct ProtocolHeader;
struct Settings;

/**
 * SocketObserver helps connection to get socket level updates without adding
 * circular dependency.
 */
struct SocketObserver {
  using observer_id_t = size_t;
  static constexpr observer_id_t kInvalidObserver = 0;
  virtual ~SocketObserver() {}
  /**
   * ProtocolHandler receives AsyncSocket notifications on the network
   * threadpool. Observers can ask ProtocolHandler to invoke one of the below
   * APIs on a specific executor instead of network threadpool.
   */
  virtual folly::Executor* getExecutor() = 0;

  /**
   * Invoked when bytes added into the ProtocolHandler have been written into
   * the underlying AsyncSocket.
   */
  virtual void onBytesWrittenToAsyncSocket(size_t nbytes) = 0;

  /**
   * Invoked when bytes added into AsyncSocket have been flushed to tcp socket.
   * We have 2 notifications for bytes written inorder to push back upper layers
   * from adding more into the AsyncSocket if we have already added enough. The
   * bytes that can be added to AsyncSocket are unbounded and it depends on the
   * user to push back in cases where the connection is slow in draining bytes.
   */
  virtual void onBytesWrittenToSocket(size_t nbytes) = 0;

  /**
   * Invoked when the underlying socket was closed because of external or
   * internal reasons.
   */
  virtual void onSocketClose(Status st) = 0;
};

/**
 * A concrete implementation of IProtocolHandler provides following
 * functionality:
 * 1. It encapsulates a unique endpoint.
 * 2. Serializes and provides transport for user to send messages to the
 *    connected endpoint.
 * 3. Receives messages from the endpoint and deserializes them.
 * 4. Routes the received message to correct WorkContext by invoking message
 *    specific handler.
 */
class IProtocolHandler {
 public:
  virtual ~IProtocolHandler() {}
  /**
   * This section includes the set of ProtocolHandler API's used by the higher
   * layers to use the socket.
   */

  /**
   * Initiates connection process for an unconnected socket and returns a
   * future. On connection success, the future completes with E::ISCONN
   * status. In case of error, status is set to the connection error.
   */
  virtual folly::Future<Status> asyncConnect(const folly::SocketAddress&,
                                             const Settings&) = 0;

  /**
   * Sends data over to the remote endpoint using the AsyncSocket. Once the data
   * is added to the AsyncSocket, SocketObserver::onBytesWrittenToSocket
   * for observer corresponding to supplied id is invoked.
   */
  virtual void sendBuffer(SocketObserver::observer_id_t id,
                          std::unique_ptr<folly::IOBuf>&& buffer_chain) = 0;

  /**
   * Flushes the buffered data before closing the socket. Read path is closed
   * right away. Once the socket is close, SocketObserver::onSocketClose is
   * invoked for all the registered observers.
   */
  virtual void close(Status) = 0;

  /**
   * Closes ProtocolHandler and the underlying socket without flushing buffered
   * data in the AsyncSocket. SocketObserver::onSocketClose is invoked for all
   * the registered observers.
   */
  virtual void closeNow(Status) = 0;

  /**
   * Return true if the socket is still usable for read and write otherwise the
   * method returns false.
   */
  virtual bool good() const = 0;

  /**
   * Register socket observer to get notified on socket events.
   * Returned id is unique to the observer and is used in various api's to
   * identify the observer and get notified.
   */
  virtual SocketObserver::observer_id_t
  registerSocketObserver(SocketObserver* o) = 0;

  /**
   * Unregister socket observer with the given id.
   */
  virtual void unregisterSocketObserver(SocketObserver::observer_id_t) = 0;

  /**
   * This section contains the set of ProtocolHandler API's that are used by the
   * AsyncSocket callbacks to send information to higher layers.
   */

  /**
   * Validate protocol header received on the socket message.
   */
  virtual bool validateProtocolHeader(const ProtocolHeader& hdr) const = 0;

  /**
   * Dispatch the received message body and the header to the appropriate work
   * context.
   */
  virtual int dispatchMessageBody(const ProtocolHeader& hdr,
                                  std::unique_ptr<folly::IOBuf> body) = 0;

  /**
   * Socket callback uses this to notify error hit on the socket. Protocol
   * Handler forwards that notification to connection usin the SocketObservers.
   */
  virtual void notifyErrorOnSocket(const folly::AsyncSocketException& err) = 0;

  /**
   * Socket callback uses this to notify proto handler bytes added into the
   * AsyncSocket have been written into tcp socket.
   */
  virtual void notifyBytesWritten() = 0;
};

}} // namespace facebook::logdevice
