/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
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
 * @file Reply to GET_RSM_SNAPSHOT_Message, sent by storage nodes to sequencer.
 */

struct GET_RSM_SNAPSHOT_REPLY_Header {
  // Status is one of:
  //   OK
  //   SHUTDOWN (server is shutting down)
  //   REDIRECT (to a node with a higher RSM version)
  //   NOTSUPPORTED (rsm is not supported)
  //   NOTFOUND (state machine not found on the server)
  //   STALE (the server's RSM version is behind what client requested)
  Status st;
  logid_t delta_log_id;
  request_id_t rqid;
  node_index_t redirect_node{-1}; // only valid when st == E::REDIRECT
  lsn_t snapshot_ver;             // snapshot version of 'snapshot_blob'
} __attribute__((__packed__));

class GET_RSM_SNAPSHOT_REPLY_Message : public Message {
 public:
  explicit GET_RSM_SNAPSHOT_REPLY_Message(
      const GET_RSM_SNAPSHOT_REPLY_Header& header,
      std::string snapshot_blob);

  GET_RSM_SNAPSHOT_REPLY_Message(const GET_RSM_SNAPSHOT_REPLY_Message&) =
      delete;
  GET_RSM_SNAPSHOT_REPLY_Message& operator=(GET_RSM_SNAPSHOT_REPLY_Message&&) =
      delete;

  const GET_RSM_SNAPSHOT_REPLY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  GET_RSM_SNAPSHOT_REPLY_Header header_;
  // Snapshot returned by the Node. This is valid only if header.st is E::OK
  std::string snapshot_blob_;
};

}} // namespace facebook::logdevice
