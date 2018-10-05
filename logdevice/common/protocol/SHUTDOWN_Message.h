/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * SHUTDOWN messages are sent by a node that is in the process of shutting down
 * to all other nodes which have an open connection.
 *
 * This is especially useful for clients since they can distinguish between
 * append errors caused by graceful shutdown and
 * ungraceful shutdown of a server
 */

struct SHUTDOWN_Header {
  Status status;
  ServerInstanceId serverInstanceId;
} __attribute__((__packed__));

class SHUTDOWN_Message : public Message {
 public:
  explicit SHUTDOWN_Message(const SHUTDOWN_Header& header);

  SHUTDOWN_Message(SHUTDOWN_Message&&) noexcept = delete;
  SHUTDOWN_Message(const SHUTDOWN_Message&) noexcept = delete;
  SHUTDOWN_Message& operator=(const SHUTDOWN_Message&) = delete;
  SHUTDOWN_Message& operator=(SHUTDOWN_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  SHUTDOWN_Header header_;
};

}} // namespace facebook::logdevice
