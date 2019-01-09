/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/NODE_STATS_REPLY_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/Sender.h"
#include "logdevice/lib/ClientWorker.h"
#include "logdevice/lib/NodeStatsMessageCallback.h"

namespace facebook { namespace logdevice {
Message::Disposition NODE_STATS_REPLY_onReceived(NODE_STATS_REPLY_Message* msg,
                                                 const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got NODE_STATS_REPLY message from client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  RATELIMIT_DEBUG(std::chrono::seconds{10},
                  1,
                  "Received a NODE_STATS_REPLY from %s",
                  from.toString().c_str());

  auto callback = ClientWorker::onThisThread()->nodeStatsMessageCallback();
  // all node stats communication should occur on the same thread, if not,
  // something went wrong
  ld_check(callback != nullptr);
  callback->onReplyFromNode(msg->getHeader().msg_id);

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
