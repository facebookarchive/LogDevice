/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

namespace facebook { namespace logdevice {

/**
 * @file Output of a message deserializer.  Opaque object meant to be created
 * only by ProtocolReader::result*() and read by the socket layer.
 */

struct Message;
class ProtocolReader;
class Socket;
struct MessageReadResult {
 public:
  std::unique_ptr<Message> msg;
  MessageReadResult(MessageReadResult&&) noexcept;
  MessageReadResult& operator=(MessageReadResult&&) noexcept;
  ~MessageReadResult();

 private:
  explicit MessageReadResult(std::unique_ptr<Message> msg);
  friend class ProtocolReader;
  friend struct Message;
};

}} // namespace facebook::logdevice
