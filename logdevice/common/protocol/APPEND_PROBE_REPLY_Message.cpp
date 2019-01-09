/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/APPEND_PROBE_REPLY_Message.h"

#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
APPEND_PROBE_REPLY_Message::onReceived(Address const& from) {
  Worker* w = Worker::onThisThread();
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "PROTOCOL ERROR: got APPEND_PROBE_REPLY message from client %s",
        Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ld_spew("Received APPEND_PROBE_REPLY from %s, log %lu, status %s, "
          "redirect %s",
          Sender::describeConnection(from).c_str(),
          header_.logid.val_,
          errorStrings()[header_.status].name,
          header_.redirect.toString().c_str());
  auto it = w->runningAppends().map.find(header_.rqid);
  if (it != w->runningAppends().map.end()) {
    it->second->onProbeReply(header_, from);
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
