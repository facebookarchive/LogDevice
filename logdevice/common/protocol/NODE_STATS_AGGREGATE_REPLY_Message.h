/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/BucketedNodeStats.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {
struct NODE_STATS_AGGREGATE_REPLY_Header {
  // should be the same value as in the request
  uint32_t msg_id{};

  uint16_t node_count{};
  uint16_t bucket_count{};
  uint16_t separate_client_count{};
};

class NODE_STATS_AGGREGATE_REPLY_Message : public Message {
 public:
  NODE_STATS_AGGREGATE_REPLY_Message(NODE_STATS_AGGREGATE_REPLY_Header header,
                                     BucketedNodeStats stats);

  virtual ~NODE_STATS_AGGREGATE_REPLY_Message() = default;

  virtual void serialize(ProtocolWriter& writer) const override;
  static MessageReadResult deserialize(ProtocolReader& reader);
  void read(ProtocolReader& reader);

  // should be implemented in server/NODE_STATS_AGGREGATE_onReceived.cpp
  virtual Disposition onReceived(const Address& from) override;

  NODE_STATS_AGGREGATE_REPLY_Header header_;
  BucketedNodeStats stats_;

 protected:
  NODE_STATS_AGGREGATE_REPLY_Message();

 private:
  void writeCountsForVersionWorstClientForBoycott(ProtocolWriter& writer) const;
  void readCountsForVersionWorstClientsForBoycott(ProtocolReader& reader);
};
}} // namespace facebook::logdevice
