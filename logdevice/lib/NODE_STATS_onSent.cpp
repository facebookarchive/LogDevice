/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Address.h"
#include "logdevice/common/protocol/NODE_STATS_Message.h"
#include "logdevice/lib/ClientWorker.h"
#include "logdevice/lib/NodeStatsMessageCallback.h"

namespace facebook { namespace logdevice {

void NODE_STATS_onSent(const NODE_STATS_Message& /*msg*/,
                       Status status,
                       const Address& /*to*/) {
  auto callback = ClientWorker::onThisThread()->nodeStatsMessageCallback();
  // all node stats communication should occur on the same thread, if not,
  // something went wrong
  ld_check(callback != nullptr);
  callback->onMessageSent(status);
}
}} // namespace facebook::logdevice
