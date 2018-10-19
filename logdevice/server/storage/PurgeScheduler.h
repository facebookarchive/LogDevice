/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <vector>

#include "logdevice/common/LogIDUniqueQueue.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  PurgeScheduler manages PurgeUncleanEpochs state machine created by
 *        RELEASE messages, ensuring only a limited number of instances can be
 *        running for each storage shard per worker at the same time. Note that
 *        currently there is no limit for PurgeUncleanEpochs machines created
 *        by CLEAN messages, which are important for the performance of epoch
 *        recovery.
 */

class ServerProcessor;

class PurgeScheduler {
 public:
  /**
   * create the PurgeScheduler object. Note that it requires that the processor
   * is running on a storage node.
   */
  explicit PurgeScheduler(ServerProcessor* processor);

  /**
   * Attempt to start PurgeUncleanEpochs state machine upon release
   *
   * @return     a valid token if the purge can be started.
   *             an invalid token if the number of active purges for release
   *             reaches the per-shard limit on the Worker. In such case, the
   *             log_id will be enqueued on the Worker so that purging can be
   *             scheduled later
   */
  ResourceBudget::Token tryStartPurgeForRelease(logid_t log_id,
                                                shard_index_t shard_index);

  /**
   * Attempt to start more purging state machines previously enqueued for a
   * storage shard.
   */
  void wakeUpMorePurgingForReleases(shard_index_t shard);

 private:
  ServerProcessor* const processor_;

  // a FIFO queue storing unique logids for purge machines waiting to be run
  std::vector<LogIDUniqueQueue> purgeForReleaseQueue_;

  // Used to limit the number of PurgeUncleanEpochs for release per storage
  // shard
  std::vector<std::unique_ptr<ResourceBudget>> purgeForReleaseBudget_;
};

}} // namespace facebook::logdevice
