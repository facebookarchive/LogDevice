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
 * @file Reply to FINDKEY_Message, sent by storage nodes to the client.
 *
 * See FindKeyRequest for a detailed explanation of the algorithm and protocol.
 */

struct FINDKEY_REPLY_Header {
  request_id_t client_rqid; // id of FindKeyRequest to deliver reply to
  Status status;            // one of: OK, AGAIN, NOTSTORAGE, FAILED, REBUILDING
  lsn_t result_lo;
  lsn_t result_hi;
  shard_index_t shard; // Shard on which we looked for that log

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class FINDKEY_REPLY_Message : public Message {
 public:
  explicit FINDKEY_REPLY_Message(const FINDKEY_REPLY_Header& header)
      : Message(MessageType::FINDKEY_REPLY, TrafficClass::READ_BACKLOG),
        header_(header) {}

  Disposition onReceived(const Address&) override;
  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  FINDKEY_REPLY_Header header_;
};

}} // namespace facebook::logdevice
