/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RecordID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file Reply to IS_LOG_EMPTY_Message, sent by storage nodes to the client.
 */

struct IS_LOG_EMPTY_REPLY_Header {
  request_id_t client_rqid;

  // Status is one of:
  //   OK
  //   SHUTDOWN
  //   NOTSTORAGE
  //   REBUILDING
  //   AGAIN
  //   FAILED
  //   NOTSUPPORTED
  //   NOTSUPPORTEDLOG
  Status status;

  bool empty;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class IS_LOG_EMPTY_REPLY_Message : public Message {
 public:
  explicit IS_LOG_EMPTY_REPLY_Message(const IS_LOG_EMPTY_REPLY_Header& header);

  IS_LOG_EMPTY_REPLY_Message(IS_LOG_EMPTY_REPLY_Message&&) noexcept = delete;
  IS_LOG_EMPTY_REPLY_Message& operator=(const IS_LOG_EMPTY_REPLY_Message&) =
      delete;
  IS_LOG_EMPTY_REPLY_Message& operator=(IS_LOG_EMPTY_REPLY_Message&&) = delete;

  const IS_LOG_EMPTY_REPLY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  IS_LOG_EMPTY_REPLY_Header header_;
};

}} // namespace facebook::logdevice
