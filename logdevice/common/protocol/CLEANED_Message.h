/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Seal.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

struct CLEANED_Header {
  // log, epoch and recovery ID that this CLEANED message is for
  logid_t log_id;
  epoch_t epoch;
  recovery_id_t recovery_id;

  // E::OK        if the last_clean_epoch counter for the strand of log .log_id
  //              has been successfully advanced to .epoch, and the per-epoch
  //              EpochRecoveryMetadata has been successfully stored in local
  //              log store.
  //
  // E::FAILED    if operation failed (will add more status codes later)
  //
  // E::PREEMPTED  the epoch recovery instance was preempted by another one
  //               with higher seal epoch.
  Status status;

  // when status is PREEMPTED, contains the seal record that preempted the CLEAN
  // message
  Seal seal;

  // Shard that this CLEANED is for.
  shard_index_t shard;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(CLEANED_Header);
  }

} __attribute__((__packed__));

class CLEANED_Message : public Message {
 public:
  explicit CLEANED_Message(const CLEANED_Header& header);

  CLEANED_Message(CLEANED_Message&&) noexcept = delete;
  CLEANED_Message(const CLEANED_Message&) noexcept = delete;
  CLEANED_Message& operator=(const CLEANED_Message&) = delete;
  CLEANED_Message& operator=(CLEANED_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  CLEANED_Header header_;
};

}} // namespace facebook::logdevice
