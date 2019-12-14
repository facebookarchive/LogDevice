/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <folly/io/async/AsyncSocket.h>

namespace folly {
class AsyncTransportCertificate;
class SocketAddress;
class IOBuf;
} // namespace folly

namespace facebook { namespace logdevice {

class SocketAdapter {
 public:
  // Alias for inherited members from AsyncReader and AsyncWriter
  // to keep compatibility.
  using ReadCallback = folly::AsyncSocket::ReadCallback;
  using WriteCallback = folly::AsyncSocket::WriteCallback;
  using ConnectCallback = folly::AsyncSocket::ConnectCallback;
  SocketAdapter() {}

  virtual ~SocketAdapter() {}
  /**
   * Initiate a connection.
   *
   * @param callback  The callback to inform when the connection attempt
   *                  completes.
   * @param address   The address to connect to.
   * @param timeout   A timeout value, in milliseconds.  If the connection
   *                  does not succeed within this period,
   *                  callback->connectError() will be invoked.
   */
  virtual void connect(ConnectCallback* callback,
                       const folly::SocketAddress& address,
                       int timeout = 0,
                       const folly::AsyncSocket::OptionMap& options =
                           folly::AsyncSocket::emptyOptionMap,
                       const folly::SocketAddress& bindAddr =
                           folly::AsyncSocket::anyAddress()) noexcept = 0;
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
  virtual void closeNow() = 0;

  /**
   * Close the socket after flushing write buffer. Stop reading immediately.
   */
  virtual void close() = 0;

  /**
   * Determine if transport is open and ready to read or write.
   *
   * Note that this function returns false on EOF; you must also call error()
   * to distinguish between an EOF and an error.
   *
   * @return  true iff the transport is open and ready, false otherwise.
   */
  virtual bool good() const = 0;

  /**
   * Determine if the transport is readable or not.
   *
   * @return  true iff the transport is readable, false otherwise.
   */
  virtual bool readable() const = 0;

  /**
   * Determine if the transport is writable or not.
   *
   * @return  true iff the transport is writable, false otherwise.
   */
  virtual bool writable() const {
    // By default return good() - leave it to implementers to override.
    return good();
  }

  /**
   * Determine if transport is connected to the endpoint
   *
   * @return  false iff the transport is connected, otherwise true
   */
  virtual bool connecting() const = 0;

  /**
   * Get the address of the local endpoint of this transport.
   *
   * This function may throw std::runtime_error on error.
   *
   * @param address  The local address will be stored in the specified
   *                 SocketAddress.
   */
  virtual void getLocalAddress(folly::SocketAddress* address) const = 0;
  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw std::runtime_error on error.
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
   * This function may throw std::runtime_error on error.
   *
   * @param address  The remote endpoint's address will be stored in the
   *                 specified SocketAddress.
   */
  virtual void getPeerAddress(folly::SocketAddress* address) const = 0;

  /**
   * Get the underlying file descriptor.
   */
  virtual folly::NetworkSocket getNetworkSocket() const = 0;

  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw std::runtime_error on error.
   *
   * @return         Return the remote endpoint's address
   */
  folly::SocketAddress getPeerAddress() const {
    folly::SocketAddress addr;
    getPeerAddress(&addr);
    return addr;
  }

  /**
   * Get the peer certificate information if any
   */
  virtual const folly::AsyncTransportCertificate* getPeerCertificate() const {
    return nullptr;
  }

  virtual size_t getRawBytesWritten() const = 0;
  virtual size_t getRawBytesReceived() const = 0;

  // Read methods that aren't part of AsyncTransport.
  virtual void setReadCB(ReadCallback* callback) = 0;
  virtual ReadCallback* getReadCallback() const = 0;

  /**
   * If you supply a non-null WriteCallback, exactly one of writeSuccess()
   * or writeErr() will be invoked when the write completes. If you supply
   * the same WriteCallback object for multiple write() calls, it will be
   * invoked exactly once per call. The only way to cancel outstanding
   * write requests is to close the socket (e.g., with closeNow() or
   * shutdownWriteNow()). When closing the socket this way, writeErr() will
   * still be invoked once for each outstanding write operation.
   */
  virtual void
  writeChain(WriteCallback* callback,
             std::unique_ptr<folly::IOBuf>&& buf,
             folly::WriteFlags flags = folly::WriteFlags::NONE) = 0;

  /**
   * Set the send bufsize
   */
  virtual int setSendBufSize(size_t bufsize) = 0;

  /**
   * Set the recv bufsize
   */
  virtual int setRecvBufSize(size_t bufsize) = 0;

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
  virtual int getSockOptVirtual(int level,
                                int optname,
                                void* optval,
                                socklen_t* optlen) = 0;
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
  virtual int setSockOptVirtual(int level,
                                int optname,
                                void const* optval,
                                socklen_t optlen) = 0;
};

}} // namespace facebook::logdevice
