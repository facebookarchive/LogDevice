/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/NODE_STATS_Message.h"

#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/NODE_STATS_REPLY_Message.h"

namespace facebook { namespace logdevice {
NODE_STATS_Message::NODE_STATS_Message(const NODE_STATS_Header& header,
                                       std::vector<NodeID> ids,
                                       append_list_t append_successes,
                                       append_list_t append_fails)
    : Message(MessageType::NODE_STATS, TrafficClass::FAILURE_DETECTOR),
      header_(header),
      ids_(ids),
      append_successes_(append_successes),
      append_fails_(append_fails) {
  ld_check(header_.num_nodes == ids_.size());
  ld_check(header_.num_nodes == append_successes_.size());
  ld_check(header_.num_nodes == append_fails_.size());
}

void NODE_STATS_Message::serialize(ProtocolWriter& writer) const {
  ld_check(header_.num_nodes == ids_.size());
  ld_check(header_.num_nodes == append_successes_.size());
  ld_check(header_.num_nodes == append_fails_.size());

  writer.write(header_);
  writer.writeVector(ids_);
  writer.writeVector(append_successes_);
  writer.writeVector(append_fails_);
}

MessageReadResult NODE_STATS_Message::deserialize(ProtocolReader& reader) {
  // don't use make_unique here because it requires a public constructor
  std::unique_ptr<NODE_STATS_Message> msg(new NODE_STATS_Message());

  reader.read(&msg->header_);
  reader.readVector(&msg->ids_, msg->header_.num_nodes);
  reader.readVector(&msg->append_successes_, msg->header_.num_nodes);
  reader.readVector(&msg->append_fails_, msg->header_.num_nodes);

  return reader.resultMsg(std::move(msg));
}

Message::Disposition NODE_STATS_Message::onReceived(const Address& from) {
  if (!from.isClientAddress()) {
    ld_error("got NODE_STATS message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  RATELIMIT_DEBUG(std::chrono::seconds{10},
                  1,
                  "Received NODE_STATS_MESSAGE from %s",
                  from.toString().c_str());

  storeReceivedStats(from.asClientID());

  sendReplyMessage(from);

  return Disposition::NORMAL;
}

void NODE_STATS_Message::onSent(Status /*status*/,
                                const Address& /*to*/) const {
  // onSent lives in lib/NODE_STATS_onSent.cpp, this should never be called
  std::abort();
}

void NODE_STATS_Message::storeReceivedStats(ClientID from) {
  ld_check(ids_.size() == append_successes_.size());
  ld_check(ids_.size() == append_fails_.size());

  auto stats = getStats();

  for (size_t i = 0; i < ids_.size(); ++i) {
    if (!dd_assert(ids_[i].isNodeID(),
                   "Received append data for non-NodeID from client %s",
                   Worker::onThisThread()
                       ->sender()
                       .describeConnection(from)
                       .c_str())) {
      continue;
    }

    PER_CLIENT_NODE_STAT_ADD(
        stats, from, ids_[i], append_successes_[i], append_fails_[i]);
  }
}

void NODE_STATS_Message::sendReplyMessage(const Address& to) {
  NODE_STATS_REPLY_Header header = {header_.msg_id};
  auto msg = std::make_unique<NODE_STATS_REPLY_Message>(header);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

StatsHolder* NODE_STATS_Message::getStats() {
  return Worker::onThisThread()->stats();
}
}} // namespace facebook::logdevice
