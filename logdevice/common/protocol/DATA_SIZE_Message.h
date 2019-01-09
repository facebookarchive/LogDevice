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
 * @file Message sent by the client library to a storage node to check
 * the approximate size of the data in a log in a given time range
 */

struct DATA_SIZE_Header {
  request_id_t client_rqid;
  logid_t log_id;
  shard_index_t shard;
  // try below, RecordTimestamp works though, in APPENDED_Message.h
  int64_t lo_timestamp_ms;
  int64_t hi_timestamp_ms;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class DATA_SIZE_Message : public Message {
 public:
  explicit DATA_SIZE_Message(const DATA_SIZE_Header& header);

  DATA_SIZE_Message(DATA_SIZE_Message&&) noexcept = delete;
  DATA_SIZE_Message& operator=(const DATA_SIZE_Message&) = delete;
  DATA_SIZE_Message& operator=(DATA_SIZE_Message&&) = delete;

  const DATA_SIZE_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  DATA_SIZE_Header header_;
};

}} // namespace facebook::logdevice
