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
 * @file Message sent by the client library to storage nodes to update the
 *       trim point for the log. Once acknowledged, storage nodes shouldn't
 *       deliver records with LSNs smaller than or equal to the trim point.
 */

struct TRIM_Header {
  request_id_t client_rqid; // id of TrimRequest; used for replies
  logid_t log_id;
  lsn_t trim_point;
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class TRIM_Message : public Message {
 public:
  explicit TRIM_Message(const TRIM_Header& header)
      : Message(MessageType::TRIM, TrafficClass::TRIM), header_(header) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address&) override;
  void onSent(Status st, const Address& to) const override;

  const TRIM_Header& getHeader() const {
    return header_;
  }

 private:
  TRIM_Header header_;
};

}} // namespace facebook::logdevice
