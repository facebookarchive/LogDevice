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
 * @file Acknowledgement of the TRIM_Message, sent by storage nodes to the
 *       client.
 */

struct TRIMMED_Header {
  request_id_t client_rqid; // id of TrimRequest to deliver reply to

  // Status is one of:
  //  * NOTSTORAGE - I'm not a storage node
  //  * ACCESS - you're not allowed to trim this log
  //  * AGAIN - something's not initialized yet, try again later
  //  * SYSLIMIT - we've hit some limit, probably don't try again
  //  * NOTFOUND - the log is not in config
  //  * SHUTDOWN - logdeviced is shutting down
  //  * INVALID_PARAM - log ID or lsn is very invalid, e.g. LOGID_INVALID or
  //                    LSN_INVALID or EPOCH_MAX
  //  * FAILED - local log store is either unwritable or overloaded
  Status status;

  shard_index_t shard;      // shard on which the log was trimmed

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class TRIMMED_Message : public Message {
 public:
  explicit TRIMMED_Message(const TRIMMED_Header& header)
      : Message(MessageType::TRIMMED, TrafficClass::TRIM), header_(header) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address&) override;

 private:
  TRIMMED_Header header_;
};

}} // namespace facebook::logdevice
