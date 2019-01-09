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
 * @file WINDOW is sent by client to update flow control sliding window
 * on storage nodes.
 */

struct Boundaries {
  lsn_t low;
  lsn_t high;
};

struct WINDOW_Header {
  logid_t log_id;                  // which log to deliver records from
  read_stream_id_t read_stream_id; // client-issued read stream ID
  Boundaries sliding_window;       // smallest and largest lsn inside
                                   // the sliding window
  shard_index_t shard;             // shard the read stream is reading from.
                                   // Set to -1 if the client uses a too old
                                   // protocol.
} __attribute__((__packed__));

class WINDOW_Message : public Message {
 public:
  /**
   * Construct a WINDOW message.
   */
  explicit WINDOW_Message(const WINDOW_Header& header);

  WINDOW_Message(WINDOW_Message&&) noexcept = delete;
  WINDOW_Message& operator=(const WINDOW_Message&) = delete;
  WINDOW_Message& operator=(WINDOW_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  bool allowUnencrypted() const override;
  static Message::deserializer_t deserialize;

  WINDOW_Header header_;
};

}} // namespace facebook::logdevice
