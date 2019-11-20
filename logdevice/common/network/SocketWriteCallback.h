/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>

#include <folly/io/async/AsyncTransport.h>

#include "logdevice/common/IProtocolHandler.h"

namespace facebook { namespace logdevice {
/**
 * SocketWriteCallback instance that can be reused across multiple AsyncSocket
 * write chain invocations. Class members not thread safe.
 */
class SocketWriteCallback : public folly::AsyncSocket::WriteCallback {
 public:
  explicit SocketWriteCallback(IProtocolHandler* conn = nullptr)
      : proto_handler_(conn) {}
  /**
   * writeSuccess() will be invoked when all of the data has been
   * successfully written.
   *
   * Note that this mainly signals that the buffer containing the data to
   * write is no longer needed and may be freed or re-used.  It does not
   * guarantee that the data has been fully transmitted to the remote
   * endpoint.  For example, on socket-based transports, writeSuccess() only
   * indicates that the data has been given to the kernel for eventual
   * transmission.
   */
  void writeSuccess() noexcept override {
    proto_handler_->notifyBytesWritten();
    ++num_success;
  }

  /**
   * writeError() will be invoked if an error occurs writing the data.
   *
   * @param bytesWritten The number of bytes that were successfull
   * @param ex           An exception describing the error that occurred.
   */
  void writeErr(size_t /* bytesWritten */,
                const folly::AsyncSocketException& ex) noexcept override {
    proto_handler_->notifyErrorOnSocket(ex);
  }

  void clear() {
    write_chains.clear();
    num_success = 0;
    bytes_buffered = 0;
  }
  struct WriteUnit {
    size_t length;
    SteadyTimestamp write_time;
  };

  // A single write callback is shared for all the writes. Hence , when write
  // success is called we do not know how many bytes were written into the
  // socket. This deque keep track of write sizes and time of write as the
  // chain was added into the socket. Time of write helps in getting the delay
  // in writing to the tcp socket.
  std::deque<WriteUnit> write_chains;

  // Cumulative size of all write_chains buffered in AsyncSocket.
  size_t bytes_buffered{0};

  // number of time write success is invoked, indicates number of write_chains_
  // that have been written into the socket.
  size_t num_success{0};

 private:
  // ProtocolHandler interface used to notify write success and errors.
  IProtocolHandler* proto_handler_;
};
}} // namespace facebook::logdevice
