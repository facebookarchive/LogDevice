/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file SHARD_STATUS_UPDATE is a message sent by a storage node to all the
 * clients that are reading from it when
 * Worker::shard_status_state::shard_status_map_ is updated  because a new event
 * was received from the event log.
 *
 * This message contains a versioned view of the state of all shards in the
 * tier. @see ShardAuthoritativeStatusMap.h for a description of such states.
 *
 * When a client receives this message, it updates its on
 * Worker::shard_status_state::shard_status_map_ with the state contained in it.
 * This is important because if the client does not get updated of the state of
 * the shards in the cluster as new events get appended to the event log,
 * it may stall trying to get an f-majority over a nodeset that has too many
 * unavailable empty nodes.
 */

struct SHARD_STATUS_UPDATE_Header {
  // Version of the shard state. This is actually the LSN of the last record in
  // the event log that changed that state.
  lsn_t version;
  // This field is deprecated, do not use.
  uint32_t num_shards_deprecated{0};
} __attribute__((__packed__));

class SHARD_STATUS_UPDATE_Message : public Message {
 public:
  explicit SHARD_STATUS_UPDATE_Message(
      const SHARD_STATUS_UPDATE_Header& header,
      ShardAuthoritativeStatusMap shard_status);

  SHARD_STATUS_UPDATE_Message(const SHARD_STATUS_UPDATE_Message&) noexcept =
      delete;
  SHARD_STATUS_UPDATE_Message(SHARD_STATUS_UPDATE_Message&&) noexcept = delete;
  SHARD_STATUS_UPDATE_Message& operator=(const SHARD_STATUS_UPDATE_Message&) =
      delete;
  SHARD_STATUS_UPDATE_Message& operator=(SHARD_STATUS_UPDATE_Message&&) =
      delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  // onSent() handler is
  // AllServerReadStreams::onShardStatusUpdateMessageSent()
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  SHARD_STATUS_UPDATE_Header header_;
  ShardAuthoritativeStatusMap shard_status_;
};

}} // namespace facebook::logdevice
