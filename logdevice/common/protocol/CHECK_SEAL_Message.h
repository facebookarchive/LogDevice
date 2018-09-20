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
 * @file
 *
 * CHECK SEAL messages are sent by sequencer node to 'r' storage nodes,
 * upon receiving a GET_SEQ_STATE Message.
 *
 * The purpose of sending this message is to check seals on storage nodes,
 * so that sequencer node can detect preemption of a log's sequencer.
 */

struct CHECK_SEAL_Header {
  // id of the CheckSealRequest sending the message; used for routing replies
  request_id_t rqid;

  // TODO (#10481839): currently unused, but will need
  // in future with server side check-seal retries.
  //
  // Needed to distinguish b/w replies from different
  // waves.
  //
  // Keeping it to avoid proto change.
  uint32_t wave;

  // log-id for which SEAL needs to be checked
  logid_t log_id;

  // NodeID of the sequencer requesting seal info for 'log_id'
  NodeID sequencer;

  // The shard on which to check the seal.
  shard_index_t shard;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(CHECK_SEAL_Header);
  }
} __attribute__((__packed__));

enum class CheckSealType { NONE, NORMAL, SOFT };

class CHECK_SEAL_Message : public Message {
 public:
  explicit CHECK_SEAL_Message(const CHECK_SEAL_Header& header);

  CHECK_SEAL_Message(const CHECK_SEAL_Message&) noexcept = delete;
  CHECK_SEAL_Message(CHECK_SEAL_Message&&) noexcept = delete;
  CHECK_SEAL_Message& operator=(const CHECK_SEAL_Message&) = delete;
  CHECK_SEAL_Message& operator=(CHECK_SEAL_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  void onSent(Status st, const Address& to) const override;
  static Message::deserializer_t deserialize;

  const CHECK_SEAL_Header& getHeader() const {
    return header_;
  }

  CHECK_SEAL_Header header_;
};

}} // namespace facebook::logdevice
