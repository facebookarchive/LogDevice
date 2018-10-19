/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

class StatsHolder;

struct NODE_STATS_AGGREGATE_Header {
  uint32_t msg_id;
  // how many buckets to get (useful if the requesting node just started up)
  uint16_t bucket_count;
};

class NODE_STATS_AGGREGATE_Message : public Message {
 public:
  explicit NODE_STATS_AGGREGATE_Message(NODE_STATS_AGGREGATE_Header header);
  virtual ~NODE_STATS_AGGREGATE_Message() = default;

  virtual void serialize(ProtocolWriter& writer) const override;
  static MessageReadResult deserialize(ProtocolReader& reader);

  virtual Disposition onReceived(const Address& from) override;

  NODE_STATS_AGGREGATE_Header header_;

 protected:
  explicit NODE_STATS_AGGREGATE_Message();
};
}} // namespace facebook::logdevice
