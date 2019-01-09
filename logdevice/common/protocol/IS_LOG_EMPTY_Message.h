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
 * wether a particular log is empty
 */

struct IS_LOG_EMPTY_Header {
  request_id_t client_rqid;
  logid_t log_id;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class IS_LOG_EMPTY_Message : public Message {
 public:
  explicit IS_LOG_EMPTY_Message(const IS_LOG_EMPTY_Header& header);

  IS_LOG_EMPTY_Message(IS_LOG_EMPTY_Message&&) noexcept = delete;
  IS_LOG_EMPTY_Message& operator=(const IS_LOG_EMPTY_Message&) = delete;
  IS_LOG_EMPTY_Message& operator=(IS_LOG_EMPTY_Message&&) = delete;

  const IS_LOG_EMPTY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  IS_LOG_EMPTY_Header header_;
};

}} // namespace facebook::logdevice
