/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file GET_EPOCH_RECOVERY_METADATA is a message sent by a storage node
 *       to other storage nodes during the purging procedure. It queries
 *       the recipient node for the EpochRecoveryMetadata for a range of
 *       epochs [start, end] for the logid, Such metadata should be
 *       persistently stored if the epoch is clean on the recipient node.
 */

struct GET_EPOCH_RECOVERY_METADATA_Header {
  // used to identify the purge state machine
  logid_t log_id;
  epoch_t purge_to;

  epoch_t start;
  uint16_t flags; // currently unused

  // Shard we are requesting EpochRecoveryMetadata from.
  shard_index_t shard;

  // Shard being purged that is requesting EpochRecoveryMetadata.
  shard_index_t purging_shard;

  // Supported since GET_EPOCH_RECOVERY_RANGE_SUPPORT
  epoch_t end;

  // Request id of the GetEpochRecoveryMetadataRequest where
  // this message originated from
  request_id_t id;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(GET_EPOCH_RECOVERY_METADATA_Header);
  }
} __attribute__((__packed__));

class GET_EPOCH_RECOVERY_METADATA_Message : public Message {
 public:
  explicit GET_EPOCH_RECOVERY_METADATA_Message(
      const GET_EPOCH_RECOVERY_METADATA_Header& header);

  GET_EPOCH_RECOVERY_METADATA_Message(
      const GET_EPOCH_RECOVERY_METADATA_Message&) noexcept = delete;
  GET_EPOCH_RECOVERY_METADATA_Message(
      GET_EPOCH_RECOVERY_METADATA_Message&&) noexcept = delete;
  GET_EPOCH_RECOVERY_METADATA_Message&
  operator=(const GET_EPOCH_RECOVERY_METADATA_Message&) = delete;
  GET_EPOCH_RECOVERY_METADATA_Message&
  operator=(GET_EPOCH_RECOVERY_METADATA_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override {
    // Receipt handler lives in
    // server/GET_EPOCH_RECOVERY_METADATA_onReceived.cpp; this should never get
    // called.
    std::abort();
  }

  const GET_EPOCH_RECOVERY_METADATA_Header& getHeader() const {
    return header_;
  }

  static Message::deserializer_t deserialize;

  GET_EPOCH_RECOVERY_METADATA_Header header_;
};

}} // namespace facebook::logdevice
