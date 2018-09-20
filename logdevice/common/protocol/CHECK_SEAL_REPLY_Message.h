/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/CHECK_SEAL_Message.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Reply to a CHECK_SEAL_Message.
 *       This is sent by storage nodes to the requesting sequencer node.
 */

struct CHECK_SEAL_REPLY_Header {
  // CheckSealRequest id, sent by sequencer node
  request_id_t rqid;

  // TODO (#10481839): currently unused, but will need
  // in future with server side check-seal retries.
  //
  // Needed to distinguish b/w replies from different
  // waves.
  //
  // Keeping it to avoid proto change.
  uint32_t wave;

  // log for which seals were checked
  logid_t log_id;

  // OK           Read at-least one seal successfully
  //
  // NOTSUPPORTED If check-seal is disabled.
  //
  // NOTSTORAGE   The node selected in copyset is not a storage node
  //
  // FAILED       If there was en error in recovering seals on storage node.
  //              Can be either a transient error (e.g. dropped RecoverSealTask)
  //              or a permanent error (e.g. fail-safed LocalLogStore).
  Status status;

  // Highest epoch among normal&soft seals present on this storage node
  // for 'log_id'
  // This field contains correct value only if 'status' == E::OK
  epoch_t sealed_epoch;

  // Type of returned seal which contains highest epoch
  // This field contains correct value only if 'status' == E::OK
  CheckSealType seal_type;

  // When status is OK, this will be set to the node id
  // which sealed 'sealed_epoch'
  // This field contains correct value only if 'status' == E::OK
  NodeID sequencer;

  // Shard that is replying.
  shard_index_t shard;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(CHECK_SEAL_REPLY_Header);
  }
} __attribute__((__packed__));

class CHECK_SEAL_REPLY_Message : public Message {
 public:
  explicit CHECK_SEAL_REPLY_Message(const CHECK_SEAL_REPLY_Header& header);

  CHECK_SEAL_REPLY_Message(const CHECK_SEAL_REPLY_Message&) noexcept = delete;
  CHECK_SEAL_REPLY_Message(CHECK_SEAL_REPLY_Message&&) noexcept = delete;
  CHECK_SEAL_REPLY_Message& operator=(const CHECK_SEAL_REPLY_Message&) = delete;
  CHECK_SEAL_REPLY_Message& operator=(CHECK_SEAL_REPLY_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  const CHECK_SEAL_REPLY_Header& getHeader() const {
    return header_;
  }

  CHECK_SEAL_REPLY_Header header_;
};

}} // namespace facebook::logdevice
