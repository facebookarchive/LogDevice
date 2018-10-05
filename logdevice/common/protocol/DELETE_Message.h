/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RecordID.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

struct DELETE_Header {
  // record to delete
  RecordID rid;

  // Wave that should be deleted. Currently unused: the record is deleted
  // regardless of its wave and flags.
  // This potentially creates race conditions, but currently it doesn't matter
  // because DELETE_Message is pretty much never used.
  // If we ever want to start deleting records again, we need to rethink this;
  // probably make all deletes conditional and apply them when flushing
  // memtables, or something like that.
  // TODO (#T10357210):
  //   Remove this (rename to something that doesn't mention wave and say that
  //   these bytes can be reused for something else now).
  uint32_t wave_DEPRECATED;

  shard_index_t shard;
} __attribute__((__packed__));

class DELETE_Message : public Message {
 public:
  /**
   * Construct a DELETE message.
   */
  explicit DELETE_Message(const DELETE_Header& header);

  DELETE_Message(DELETE_Message&&) noexcept = delete;
  DELETE_Message& operator=(const DELETE_Message&) = delete;
  DELETE_Message& operator=(DELETE_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override;
  static Message::deserializer_t deserialize;

  const DELETE_Header& getHeader() const {
    return header_;
  }

  DELETE_Header header_;
};

}} // namespace facebook::logdevice
