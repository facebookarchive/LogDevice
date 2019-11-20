/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <fizz/server/AsyncFizzServer.h>
#include <fizz/server/FizzServerContext.h>
#include <folly/io/async/AsyncSocket.h>

#include "logdevice/common/network/SocketAdapter.h"

namespace folly {
class EventBase;
class SocketAddress;
struct NetworkSocket;
class SSLContext;
} // namespace folly

namespace facebook { namespace logdevice {

class AsyncSocketAdapter
    : public SocketAdapter,
      public fizz::server::AsyncFizzServer::HandshakeCallback {
 public:
  AsyncSocketAdapter();
  /**
   * Create a new unconnected AsyncSocket.
   *
   * connect() must later be called on this socket to establish a connection.
   */
  explicit AsyncSocketAdapter(folly::EventBase* evb);

  /**
   * Create a new AsyncSocket and begin the connection process.
   *
   * @param evb             EventBase that will manage this socket.
   * @param address         The address to connect to.
   * @param connectTimeout  Optional timeout in milliseconds for the connection
   *                        attempt.
   */
  AsyncSocketAdapter(folly::EventBase* evb,
                     const folly::SocketAddress& address,
                     uint32_t connectTimeout = 0);

  /**
   * Create a client AsyncSSLSocket
   */
  AsyncSocketAdapter(const std::shared_ptr<folly::SSLContext>& ctx,
                     folly::EventBase* evb);

  /**
   * Create a new AsyncSocket and begin the connection process.
   *
   * @param evb             EventBase that will manage this socket.
   * @param ip              IP address to connect to (dotted-quad).
   * @param port            Destination port in host byte order.
   * @param connectTimeout  Optional timeout in milliseconds for the connection
   *                        attempt.
   */
  AsyncSocketAdapter(folly::EventBase* evb,
                     const std::string& ip,
                     uint16_t port,
                     uint32_t connectTimeout = 0);

  /**
   * Create a AsyncSocket from an already connected socket file descriptor.
   *
   * Note that while AsyncSocket enables TCP_NODELAY for sockets it creates
   * when connecting, it does not change the socket options when given an
   * existing file descriptor.  If callers want TCP_NODELAY enabled when using
   * this version of the constructor, they need to explicitly call
   * setNoDelay(true) after the constructor returns.
   *
   * @param evb EventBase that will manage this socket.
   * @param fd  File descriptor to take over (should be a connected socket).
   * @param zeroCopyBufId Zerocopy buf id to start with.
   */
  AsyncSocketAdapter(folly::EventBase* evb,
                     folly::NetworkSocket fd,
                     uint32_t zeroCopyBufId = 0);

  /**
   * Create a server AsyncSSLSocket from an already connected
   * socket file descriptor.
   *
   * Tries fizz (TLS 1.3) first, then fallbacks to openssl.
   *
   * Note that while AsyncSSLSocket enables TCP_NODELAY for sockets it creates
   * when connecting, it does not change the socket options when given an
   * existing file descriptor.  If callers want TCP_NODELAY enabled when using
   * this version of the constructor, they need to explicitly call
   * setNoDelay(true) after the constructor returns.
   *
   * @param ctx  SSL context for this connection.
   * @param evb  EventBase that will manage this socket.
   * @param fd   File descriptor to take over (should be a connected socket).
   */
  AsyncSocketAdapter(
      const std::shared_ptr<const fizz::server::FizzServerContext>& fizzCtx,
      const std::shared_ptr<folly::SSLContext>& sslCtx,
      folly::EventBase* evb,
      folly::NetworkSocket fd);

  ~AsyncSocketAdapter() override;

  void connect(ConnectCallback* callback,
               const folly::SocketAddress& address,
               int timeout = 0,
               const folly::AsyncSocket::OptionMap& options =
                   folly::AsyncSocket::emptyOptionMap,
               const folly::SocketAddress& bindAddr =
                   folly::AsyncSocket::anyAddress()) noexcept override;
  /**
   * Close the transport immediately.
   *
   * This closes the transport immediately, dropping any outstanding data
   * waiting to be written.
   *
   * If a read callback is set, readEOF() will be called immediately.
   * If there are outstanding write requests, these requests will be aborted
   * and writeError() will be invoked immediately on all outstanding write
   * callbacks.
   */
  void closeNow() override;

  /**
   * Close the socket after flushing write buffer. Stop reading immediately.
   */
  void close() override;

  /**
   * Determine if transport is open and ready to read or write.
   *
   * Note that this function returns false on EOF; you must also call error()
   * to distinguish between an EOF and an error.
   *
   * @return  true iff the transport is open and ready, false otherwise.
   */
  bool good() const override;

  /**
   * Determine if the transport is readable or not.
   *
   * @return  true iff the transport is readable, false otherwise.
   */
  bool readable() const override;

  /**
   * Determine if the transport is writable or not.
   *
   * @return  true iff the transport is writable, false otherwise.
   */
  bool writable() const override {
    // By default return good() - leave it to implementers to override.
    return good();
  }

  /**
   * Determine if transport is connected to the endpoint
   *
   * @return  false iff the transport is connected, otherwise true
   */
  bool connecting() const override;

  /**
   * Get the address of the local endpoint of this transport.
   *
   * This function may throw AsyncSocketException on error.
   *
   * @param address  The local address will be stored in the specified
   *                 SocketAddress.
   */
  void getLocalAddress(folly::SocketAddress* address) const override;
  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw AsyncSocketException on error.
   *
   * @return         Return the local address
   */
  folly::SocketAddress getLocalAddress() const {
    folly::SocketAddress addr;
    getLocalAddress(&addr);
    return addr;
  }
  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw AsyncSocketException on error.
   *
   * @param address  The remote endpoint's address will be stored in the
   *                 specified SocketAddress.
   */
  void getPeerAddress(folly::SocketAddress* address) const override;

  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw AsyncSocketException on error.
   *
   * @return         Return the remote endpoint's address
   */
  folly::SocketAddress getPeerAddress() const {
    folly::SocketAddress addr;
    getPeerAddress(&addr);
    return addr;
  }

  /**
   * @return Get the underlying file description used by the async socket.
   */
  folly::NetworkSocket getNetworkSocket() const override;

  /**
   * Get the peer certificate information if any
   */
  const folly::AsyncTransportCertificate* getPeerCertificate() const override;

  size_t getRawBytesWritten() const override;
  size_t getRawBytesReceived() const override;

  // Read methods that aren't part of AsyncTransport.
  void setReadCB(SocketAdapter::ReadCallback* callback) override;
  SocketAdapter::ReadCallback* getReadCallback() const override;

  /**
   * If you supply a non-null WriteCallback, exactly one of writeSuccess()
   * or writeErr() will be invoked when the write completes. If you supply
   * the same WriteCallback object for multiple write() calls, it will be
   * invoked exactly once per call. The only way to cancel outstanding
   * write requests is to close the socket (e.g., with closeNow() or
   * shutdownWriteNow()). When closing the socket this way, writeErr() will
   * still be invoked once for each outstanding write operation.
   */
  void writeChain(WriteCallback* callback,
                  std::unique_ptr<folly::IOBuf>&& buf,
                  folly::WriteFlags flags = folly::WriteFlags::NONE) override;

  /**
   * Set the send bufsize
   */
  int setSendBufSize(size_t bufsize) override;

  /**
   * Set the recv bufsize
   */
  int setRecvBufSize(size_t bufsize) override;

  /**
   * Virtual method for reading a socket option returning integer
   * value, which is the most typical case. Convenient for overriding
   * and mocking.
   *
   * @param level     same as the "level" parameter in getsockopt().
   * @param optname   same as the "optname" parameter in getsockopt().
   * @param optval    same as "optval" parameter in getsockopt().
   * @param optlen    same as "optlen" parameter in getsockopt().
   * @return          same as the return value of getsockopt().
   */
  int getSockOptVirtual(int level,
                        int optname,
                        void* optval,
                        socklen_t* optlen) override;
  /**
   * Virtual method for setting a socket option accepting integer
   * value, which is the most typical case. Convenient for overriding
   * and mocking.
   *
   * @param level     same as the "level" parameter in setsockopt().
   * @param optname   same as the "optname" parameter in setsockopt().
   * @param optval    same as "optval" parameter in setsockopt().
   * @param optlen    same as "optlen" parameter in setsockopt().
   * @return          same as the return value of setsockopt().
   */
  int setSockOptVirtual(int level,
                        int optname,
                        void const* optval,
                        socklen_t optlen) override;

 private:
  void fizzHandshakeSuccess(
      fizz::server::AsyncFizzServer* transport) noexcept override;
  void fizzHandshakeError(fizz::server::AsyncFizzServer* transport,
                          folly::exception_wrapper ex) noexcept override;
  void fizzHandshakeAttemptFallback(
      std::unique_ptr<folly::IOBuf> clientHello) override;
  fizz::server::AsyncFizzServer* toServer() const;
  folly::AsyncSocket* toSocket() const;

  folly::AsyncTransportWrapper::UniquePtr transport_;
  std::shared_ptr<folly::SSLContext> sslCtx_;
};

}} // namespace facebook::logdevice
