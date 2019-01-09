/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file STOP is sent by readers to request that a server stop delivering
 *       records.
 */

struct STOP_Header {
  logid_t log_id;                  // which log to stop delivering records from
  read_stream_id_t read_stream_id; // client-issued read stream ID
  shard_index_t shard;             // shard the read stream is reading from.
                                   // Set to -1 if the client uses a too old
                                   // protocol.
} __attribute__((__packed__));

class STOP_Message : public Message {
 public:
  /**
   * Construct a STOP message.
   */
  explicit STOP_Message(const STOP_Header& header);

  STOP_Message(STOP_Message&&) noexcept = delete;
  STOP_Message& operator=(const STOP_Message&) = delete;
  STOP_Message& operator=(STOP_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  bool allowUnencrypted() const override;
  static Message::deserializer_t deserialize;

  const STOP_Header& getHeader() const {
    return header_;
  }

  STOP_Header header_;
};

}} // namespace facebook::logdevice
