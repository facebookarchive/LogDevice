/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/NODE_STATS_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/NODE_STATS_REPLY_Message.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/sequencer_boycotting/BoycottingStats.h"

namespace facebook { namespace logdevice {

Message::Disposition NODE_STATS_onReceived(NODE_STATS_Message* msg,
                                           const Address& from) {
  ld_check(msg);
  if (!from.isClientAddress()) {
    ld_error("got NODE_STATS message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  RATELIMIT_DEBUG(std::chrono::seconds{10},
                  1,
                  "Received NODE_STATS_MESSAGE from %s",
                  from.toString().c_str());

  ld_check(msg->ids_.size() == msg->append_successes_.size());
  ld_check(msg->ids_.size() == msg->append_fails_.size());

  auto stats = ServerWorker::onThisThread()->getBoycottingStats();
  ld_check(stats);

  for (size_t i = 0; i < msg->ids_.size(); ++i) {
    if (!dd_assert(msg->ids_[i].isNodeID(),
                   "Received append data for non-NodeID from client %s",
                   Worker::onThisThread()
                       ->sender()
                       .describeConnection(from)
                       .c_str())) {
      continue;
    }

    perClientNodeStatAdd(stats,
                         from.asClientID(),
                         msg->ids_[i],
                         msg->append_successes_[i],
                         msg->append_fails_[i]);
  }

  msg->sendReplyMessage(from);

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
