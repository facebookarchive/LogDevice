/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClientID.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file MUTATED message is sent as a reply to a mutation request. A mutation
 *       is a STORE message sent as a part of log recovery (it has the
 *       RECOVERY flag set).
 */

struct MUTATED_Header {
  // unique ID of the epoch recovery machine which sent the original message
  recovery_id_t recovery_id;

  // ID of the record this message refers to
  RecordID rid;

  // @see STORED_Header::status
  Status status;

  // when status is PREEMPTED, this will be set to seal record of
  // the most recent sequencer that sealed this log on the storage node that
  // sends this message
  Seal seal;

  shard_index_t shard;

  // Recovery wave. Not to be confused with recovery epoch. Zero if the message
  // was sent by an older version of logdevice that doesn't support it.
  // TODO (T11866467): make it required.
  uint32_t wave;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return proto >= Compatibility::WAVE_IN_MUTATED
        ? sizeof(MUTATED_Header)
        : offsetof(MUTATED_Header, wave);
  }

} __attribute__((__packed__));

class MUTATED_Message : public Message {
 public:
  explicit MUTATED_Message(const MUTATED_Header& header);

  MUTATED_Message(MUTATED_Message&&) noexcept = delete;
  MUTATED_Message(const MUTATED_Message&) noexcept = delete;
  MUTATED_Message& operator=(const MUTATED_Message&) = delete;
  MUTATED_Message& operator=(MUTATED_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  /**
   * Creates a MUTATED message and sends it to the specified client. Logs an
   * error of failure.
   */
  static void createAndSend(const MUTATED_Header& header, ClientID send_to);

  MUTATED_Header header_;
};

}} // namespace facebook::logdevice
