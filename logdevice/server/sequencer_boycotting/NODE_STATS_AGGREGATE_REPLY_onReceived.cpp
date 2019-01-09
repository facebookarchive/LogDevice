/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/NODE_STATS_AGGREGATE_REPLY_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerCallback.h"

namespace facebook { namespace logdevice {
Message::Disposition
NODE_STATS_AGGREGATE_REPLY_onReceived(NODE_STATS_AGGREGATE_REPLY_Message* msg,
                                      const Address& from) {
  auto callback = ServerWorker::onThisThread()->nodeStatsControllerCallback();
  dd_assert(callback,
            "Received a NODE_STATS_AGGREGATE_REPLY from %s to a worker not "
            "responsible for the NodeStatsController.",
            Sender::describeConnection(from).c_str());

  // the message should only be received after having requested it, and should
  // come back to the same worker
  if (callback) {
    callback->onStatsReceived(
        msg->header_.msg_id, from.asNodeID(), std::move(msg->stats_));
  }

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
