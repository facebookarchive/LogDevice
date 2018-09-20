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
 * @file Reply to DATA_SIZE_Message, sent by storage nodes to the client.
 */

struct DATA_SIZE_REPLY_Header {
  request_id_t client_rqid;

  // Status is one of:
  //   OK
  //   SHUTDOWN
  //   NOTSTORAGE
  //   REBUILDING
  //   AGAIN
  //   FAILED
  //   NOTSUPPORTED
  Status status;

  size_t size;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class DATA_SIZE_REPLY_Message : public Message {
 public:
  explicit DATA_SIZE_REPLY_Message(const DATA_SIZE_REPLY_Header& header);

  DATA_SIZE_REPLY_Message(DATA_SIZE_REPLY_Message&&) noexcept = delete;
  DATA_SIZE_REPLY_Message& operator=(const DATA_SIZE_REPLY_Message&) = delete;
  DATA_SIZE_REPLY_Message& operator=(DATA_SIZE_REPLY_Message&&) = delete;

  const DATA_SIZE_REPLY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  DATA_SIZE_REPLY_Header header_;
};

}} // namespace facebook::logdevice
