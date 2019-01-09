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
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by the client library to a storage node to get
 * its view of the trim point and approximate timestamp of trim point of the
 * given log.
 */

struct GET_HEAD_ATTRIBUTES_Header {
  request_id_t client_rqid;
  logid_t log_id;
  uint32_t flags; // currently unused
  shard_index_t shard;
  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class GET_HEAD_ATTRIBUTES_Message : public Message {
 public:
  explicit GET_HEAD_ATTRIBUTES_Message(
      const GET_HEAD_ATTRIBUTES_Header& header);

  GET_HEAD_ATTRIBUTES_Message(GET_HEAD_ATTRIBUTES_Message&&) noexcept = delete;
  GET_HEAD_ATTRIBUTES_Message& operator=(const GET_HEAD_ATTRIBUTES_Message&) =
      delete;
  GET_HEAD_ATTRIBUTES_Message& operator=(GET_HEAD_ATTRIBUTES_Message&&) =
      delete;

  const GET_HEAD_ATTRIBUTES_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  GET_HEAD_ATTRIBUTES_Header header_;
};

}} // namespace facebook::logdevice
