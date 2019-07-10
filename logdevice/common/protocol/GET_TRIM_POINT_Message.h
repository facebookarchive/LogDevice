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
 * @file Message sent by the sequencer to a storage node to get the trim point
 * of a particular log
 */

struct GET_TRIM_POINT_Header {
  logid_t log_id;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class GET_TRIM_POINT_Message : public Message {
 public:
  explicit GET_TRIM_POINT_Message(const GET_TRIM_POINT_Header& header);

  GET_TRIM_POINT_Message(const GET_TRIM_POINT_Message&) = delete;
  GET_TRIM_POINT_Message(GET_TRIM_POINT_Message&&) noexcept = delete;
  GET_TRIM_POINT_Message& operator=(const GET_TRIM_POINT_Message&) = delete;
  GET_TRIM_POINT_Message& operator=(GET_TRIM_POINT_Message&&) = delete;

  const GET_TRIM_POINT_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

 private:
  GET_TRIM_POINT_Header header_;
};

}} // namespace facebook::logdevice
