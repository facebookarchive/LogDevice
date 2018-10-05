/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Sent from a client to a random node in the cluster containing the append
 * success and append fail stats for all nodes that the client sending
 */

struct NODE_STATS_Header {
  // used for acknowledning that a message was received at the node
  // The node should use the same id when replying with the
  // NODE_STATS_REPLY_Message
  uint64_t msg_id;

  // the amount of nodes which have stats recorded for them in this message
  uint16_t num_nodes;
} __attribute__((__packed__));

class NODE_STATS_Message : public Message {
 public:
  using append_list_t = std::vector<uint32_t>;
  NODE_STATS_Message(const NODE_STATS_Header& header,
                     std::vector<NodeID> ids,
                     append_list_t append_successes,
                     append_list_t append_fails);

  void serialize(ProtocolWriter& writer) const override;
  static MessageReadResult deserialize(ProtocolReader& reader);

  Disposition onReceived(const Address& from) override;
  // should not be called, only call in lib/NODE_STATS_onSent.cpp
  void onSent(Status status, const Address& to) const override;

  // It gets very verbose because of the retries when trying to send a message
  // Instead let the NodeStatsHandler care about the mismatch in protocols
  bool warnAboutOldProtocol() const override {
    return false;
  }

  NODE_STATS_Header header_;
  // all vectors must have the the same size
  // should be considered as map<NodeID, uint32_t>, but is sent as a vectors
  // for easier serialization
  std::vector<NodeID> ids_;
  append_list_t append_successes_;
  append_list_t append_fails_;

 protected: // for tests
  // used when deserializing or testing
  NODE_STATS_Message()
      : Message(MessageType::NODE_STATS, TrafficClass::FAILURE_DETECTOR) {}

  virtual void sendReplyMessage(const Address& to);

  virtual StatsHolder* getStats();

 private:
  void storeReceivedStats(ClientID from);
};
}} // namespace facebook::logdevice
