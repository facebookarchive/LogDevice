/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

namespace folly {
class IOBuf;
class AsyncSocketException;
} // namespace folly

namespace facebook { namespace logdevice {
struct ProtocolHeader;
struct Settings;
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
  virtual bool validateProtocolHeader(const ProtocolHeader& hdr) const = 0;

  virtual int dispatchMessageBody(const ProtocolHeader& hdr,
                                  std::unique_ptr<folly::IOBuf> body) = 0;
  virtual void notifyErrorOnSocket(const folly::AsyncSocketException& err) = 0;
  virtual void notifyBytesWritten(size_t nbytes) = 0;
};

}} // namespace facebook::logdevice
