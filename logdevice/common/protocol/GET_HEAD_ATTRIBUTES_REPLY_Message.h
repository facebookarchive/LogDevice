/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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
 * @file Reply to GET_HEAD_ATTRIBUTES_Message, sent by storage nodes
 * to the client.
 */

/**
 * @param trim_point             Local node view on log trim point.
 * @param trim_point_timestamp   Will be set as
 *                               std::chrono::milliseconds::max() if timestamp
 *                               of trim point was not found because no records
 *                               bigger than local trim point found.
 * @param status   Status of the executed GET_HEAD_ATTRIBUTES_Message
 * The possible statuses could be:
 * E::OK           Trim point was successfully found. Its approximate timestamp
 *                 is found or reported as missing on this node.
 * E::REBUILDING   Shard is under rebuilding.
 * E::NOTSTORAGE   This is not a storage node.
 * E::NOTSUPPORTED Underlying storage does not support this API.
 * E::DROPPED      Node is overloaded with requests. May get better soon.
 * E::FAILED       Critical failure.
 */
struct GET_HEAD_ATTRIBUTES_REPLY_Header {
  request_id_t client_rqid;
  Status status;
  lsn_t trim_point;
  uint64_t trim_point_timestamp;
  uint32_t flags; // currently unused
  shard_index_t shard;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);
} __attribute__((__packed__));

class GET_HEAD_ATTRIBUTES_REPLY_Message : public Message {
 public:
  explicit GET_HEAD_ATTRIBUTES_REPLY_Message(
      const GET_HEAD_ATTRIBUTES_REPLY_Header& header);

  GET_HEAD_ATTRIBUTES_REPLY_Message(
      GET_HEAD_ATTRIBUTES_REPLY_Message&&) noexcept = delete;
  GET_HEAD_ATTRIBUTES_REPLY_Message&
  operator=(const GET_HEAD_ATTRIBUTES_REPLY_Message&) = delete;
  GET_HEAD_ATTRIBUTES_REPLY_Message&
  operator=(GET_HEAD_ATTRIBUTES_REPLY_Message&&) = delete;

  const GET_HEAD_ATTRIBUTES_REPLY_Header& getHeader() const {
    return header_;
  }

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  GET_HEAD_ATTRIBUTES_REPLY_Header header_;
};

}} // namespace facebook::logdevice
