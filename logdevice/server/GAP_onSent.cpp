/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GAP_onSent.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/storage/AllCachedDigests.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

void GAP_onSent(const GAP_Message& msg,
                Status st,
                const Address& to,
                const SteadyTimestamp enqueue_time) {
  if (st != E::OK) {
    // If the message failed to send, the messaging layer will be closing the
    // socket, so there's no point in trying to send more messages into it.
    ld_debug("GAP message to %s failed to send: %s",
             Sender::describeConnection(to).c_str(),
             error_description(st));
    return;
  }

  ServerWorker* w = ServerWorker::onThisThread();
  WORKER_TRAFFIC_CLASS_STAT_INCR(msg.tc_, gap_messages_sent);
  if (msg.source_ == GAP_Message::Source::CACHED_DIGEST) {
    w->cachedDigests().onGapSent(to.id_.client_, msg);
  } else {
    w->serverReadStreams().onGapSent(to.id_.client_, msg, enqueue_time);
  }
}

}} // namespace facebook::logdevice
