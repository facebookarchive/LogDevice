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
 * @file Reply to GET_TRIM_POINT_Message, sent by storage nodes to sequencer.
 */

struct GET_TRIM_POINT_REPLY_Header {
  // Status is one of:
  //   OK
  //   SHUTDOWN
  //   NOTSTORAGE
  //   REBUILDING
  //   AGAIN
  //   FAILED
  Status status;

  logid_t log_id;
  lsn_t trim_point;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class GET_TRIM_POINT_REPLY_Message : public Message {
 public:
  explicit GET_TRIM_POINT_REPLY_Message(
      const GET_TRIM_POINT_REPLY_Header& header);

  GET_TRIM_POINT_REPLY_Message(const GET_TRIM_POINT_REPLY_Message&) = delete;
  GET_TRIM_POINT_REPLY_Message(GET_TRIM_POINT_REPLY_Message&&) = delete;
  GET_TRIM_POINT_REPLY_Message& operator=(const GET_TRIM_POINT_REPLY_Message&) =
      delete;
  GET_TRIM_POINT_REPLY_Message& operator=(GET_TRIM_POINT_REPLY_Message&&) =
      delete;

  const GET_TRIM_POINT_REPLY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  GET_TRIM_POINT_REPLY_Header header_;
};

}} // namespace facebook::logdevice
