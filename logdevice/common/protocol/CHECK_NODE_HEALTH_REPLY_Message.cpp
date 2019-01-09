/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_REPLY_Message.h"

#include "logdevice/common/CheckNodeHealthRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
CHECK_NODE_HEALTH_REPLY_Message::onReceived(const Address& from) {
  Worker* w = Worker::onThisThread();
  ld_check(w);

  ld_debug("Got an CHECK_NODE_HEALTH_REPLY message from %s. rqid = %" PRIu64
           ", status = %s",
           Sender::describeConnection(from).c_str(),
           header_.rid.val_,
           error_name(header_.status));

  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got "
                    "CHECK_NODE_HEALTH_REPLY message from client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  // Is the request still valid?
  auto& pending_health_check_ = w->pendingHealthChecks().set;
  const auto& index =
      pending_health_check_.get<CheckNodeHealthRequestSet::RequestIndex>();
  auto it = index.find(header_.rid);

  if (it == index.end()) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "CHECK_NODE_HEALTH_REPLY message"
                   "received for a request that no longer exists from %s",
                   Sender::describeConnection(from).c_str());
    return Disposition::NORMAL;
  }

  (*it)->onReply(header_.status);
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
