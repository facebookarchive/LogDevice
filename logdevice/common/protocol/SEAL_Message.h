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
 * @file  SEAL messages are sent by sequencers during recovery and are used to
 *        indicate that storage nodes should no longer take writes for any
 *        epoch up to and including the one specified in the message.
 */

struct SEAL_Header {
  // id of the request sending the message; used for routing replies
  request_id_t rqid;

  // log to seal
  logid_t log_id;

  // seal the log up to and including this epoch
  epoch_t seal_epoch = EPOCH_INVALID;

  // last clean epoch for `log_id' at the time of sealing
  epoch_t last_clean_epoch = EPOCH_INVALID;

  // id of the sequencer sealing the log
  NodeID sealed_by;

  // shard to seal.
  shard_index_t shard;

} __attribute__((__packed__));

class SEAL_Message : public Message {
 public:
  explicit SEAL_Message(const SEAL_Header& header);

  SEAL_Message(SEAL_Message&&) noexcept = delete;
  SEAL_Message& operator=(const SEAL_Message&) = delete;
  SEAL_Message& operator=(SEAL_Message&&) = delete;

  const SEAL_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  SEAL_Header header_;
};

}} // namespace facebook::logdevice
