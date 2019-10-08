/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RetryHandler.h"

#include <folly/Memory.h>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

std::unique_ptr<BackoffTimer> RetryHandler::createRetryTimer(ShardID shard) {
  return std::make_unique<ExponentialBackoffTimer>(

      [this, shard] { this->execute(shard); },
      retry_initial_delay_,
      retry_max_delay_);
}

std::shared_ptr<ServerConfig> RetryHandler::getConfig() const {
  return Worker::onThisThread()->getConfig()->serverConfig();
}

void RetryHandler::activateTimer(ShardID to, bool reset) {
  std::unique_ptr<BackoffTimer>& timer = retry_timers_[to];

  if (!timer) {
    timer = createRetryTimer(to);
  }

  if (reset) {
    timer->reset();
  }

  timer->activate();
}

}} // namespace facebook::logdevice
