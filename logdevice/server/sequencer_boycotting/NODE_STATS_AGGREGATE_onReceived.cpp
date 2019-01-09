/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/NODE_STATS_AGGREGATE_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_REPLY_Message.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerCallback.h"
#include "logdevice/server/sequencer_boycotting/PerClientNodeStatsAggregator.h"

namespace facebook { namespace logdevice {

Message::Disposition
NODE_STATS_AGGREGATE_onReceived(NODE_STATS_AGGREGATE_Message* msg,
                                const Address& from) {
  if (!from.isClientAddress()) {
    ld_error("got NODE_STATS_AGGREGATE message from node %s, expected it to "
             "come from a client",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  RATELIMIT_DEBUG(std::chrono::seconds{10},
                  1,
                  "Responding to %s with node stats",
                  from.toString().c_str());

  auto stats =
      PerClientNodeStatsAggregator{}.aggregate(msg->header_.bucket_count);

  NODE_STATS_AGGREGATE_REPLY_Header header;
  header.msg_id = msg->header_.msg_id;
  header.node_count = stats.node_ids.size();
  header.bucket_count = msg->header_.bucket_count;
  header.separate_client_count = stats.client_counts->shape()[2];

  auto response_msg = std::make_unique<NODE_STATS_AGGREGATE_REPLY_Message>(
      std::move(header), std::move(stats));
  Worker::onThisThread()->sender().sendMessage(std::move(response_msg), from);

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
