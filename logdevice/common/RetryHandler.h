/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

/**
 * @file RetryHandler is a utility class used to perform an action on a
 *       storage shard (e.g. sending a message), retrying later (with a backoff)
 *       in case of a failure.
 */

class RetryHandler {
 public:
  typedef std::function<int(ShardID)> func_t;

  explicit RetryHandler(func_t handler, // handler should return 0 on success
                        std::chrono::milliseconds retry_initial_delay =
                            std::chrono::milliseconds(5),
                        std::chrono::milliseconds retry_max_delay =
                            std::chrono::milliseconds(1000))
      : handler_(handler),
        retry_initial_delay_(retry_initial_delay),
        retry_max_delay_(retry_max_delay) {
    ld_check(handler);
  }

  virtual ~RetryHandler() {}

  /**
   * Activate the timer that calls handler_.
   *
   * @param shard  shard to activate the timer for
   * @param reset  if set, timeout will be reset to its initial value
   */
  virtual void activateTimer(ShardID shard, bool reset = false);

  // Call handler_ and activate the timer in case of failure
  virtual int execute(ShardID shard) {
    int rv = handler_(shard);
    if (rv != 0) {
      activateTimer(shard);
    }
    return rv;
  }

 protected:
  // default constructor, used in tests
  explicit RetryHandler() {}

 private:
  std::shared_ptr<ServerConfig> getConfig() const;
  std::unique_ptr<BackoffTimer> createRetryTimer(ShardID shard);

  func_t handler_;
  std::chrono::milliseconds retry_initial_delay_;
  std::chrono::milliseconds retry_max_delay_;

  // Retry timers for connection or transient errors
  std::unordered_map<ShardID, std::unique_ptr<BackoffTimer>, ShardID::Hash>
      retry_timers_;
};

}} // namespace facebook::logdevice
