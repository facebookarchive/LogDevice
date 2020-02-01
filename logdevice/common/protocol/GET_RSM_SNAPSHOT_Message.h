/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by a client to a server node to get the in memory
 * view of a specific RSM.
 */

using GET_RSM_SNAPSHOT_flags_t = uint8_t;

struct GET_RSM_SNAPSHOT_Header {
  lsn_t min_ver;
  request_id_t rqid;
  GET_RSM_SNAPSHOT_flags_t flags;
} __attribute__((__packed__));

class GET_RSM_SNAPSHOT_Message : public Message {
 public:
  explicit GET_RSM_SNAPSHOT_Message(const GET_RSM_SNAPSHOT_Header& header,
                                    std::string key);

  // Force the recipient server to return it's RSM state instead of redirecting
  // the client to some other cluster node.
  static const GET_RSM_SNAPSHOT_flags_t FORCE = 1 << 0;

  GET_RSM_SNAPSHOT_Message(const GET_RSM_SNAPSHOT_Message&) = delete;
  GET_RSM_SNAPSHOT_Message(GET_RSM_SNAPSHOT_Message&&) noexcept = delete;
  GET_RSM_SNAPSHOT_Message& operator=(const GET_RSM_SNAPSHOT_Message&) = delete;
  GET_RSM_SNAPSHOT_Message& operator=(GET_RSM_SNAPSHOT_Message&&) = delete;

  const GET_RSM_SNAPSHOT_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  void onSent(Status st, const Address& to) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  GET_RSM_SNAPSHOT_Header header_;
  // RSM type: this is the delta log id of the state machine e.g.
  // Logsconfig, EventLog etc.
  std::string key_;
};

}} // namespace facebook::logdevice
