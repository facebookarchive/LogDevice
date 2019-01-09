/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DomainIsolationUpdatedRequest.h"

#include "logdevice/common/Appender.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

Request::Execution DomainIsolationUpdatedRequest::execute() {
  if (isolated_) {
    Worker* worker = Worker::onThisThread(false);
    if (worker->idx_.val() == 0) {
      worker->activateIsolationTimer();
    }
    return Execution::COMPLETE;
  }

  Worker* worker = Worker::onThisThread();
  worker->deactivateIsolationTimer();

  // Resets all the store timer for appenders that need multiple domains of
  // this scope to replicate. Presumably the appender was stuck during the
  // isolation, unable to select a copyset, and hopefully now it will be able
  // to select copyset. Of course it's far from guaranteed: (a) appender may
  // need more than two domains available, (b) appender's nodeset may not
  // contain enough available nodes. But this best-effort heuristic works in
  // the most common situation: domain rejoining the cluster after a network
  // partition; in that situation most of the cluster becomes available at once.
  for (auto& appender : worker->activeAppenders().map) {
    if (scope_ == appender.getBiggestReplicationScope()) {
      if (appender.storeTimerIsActive()) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "Resetting store timer for appender %s because "
                       "scope %s is no longer isolated",
                       appender.store_hdr_.rid.toString().c_str(),
                       NodeLocation::scopeNames()[scope_].c_str());
        appender.fireStoreTimer();
        WORKER_STAT_INCR(appender_store_timer_reset);
      }
    }
  }

  return Execution::COMPLETE;
}
}} // namespace facebook::logdevice
