/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AbortAppendersEpochRequest.h"

#include "logdevice/common/Appender.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

Request::Execution AbortAppendersEpochRequest::execute() {
  Worker* worker = Worker::onThisThread();
  std::vector<Appender*> appenders_to_abort;
  for (auto& appender : worker->activeAppenders().map) {
    auto addAppenderToList = appender.getLogID() == log_id_ &&
        lsn_to_epoch(appender.getLSN()) == epoch_;

    if (addAppenderToList ||
        (log_id_ == LOGID_INVALID && epoch_ == EPOCH_INVALID)) {
      ld_check(appender.started());
      appenders_to_abort.push_back(&appender);
    }
  }

  // Note: first collect appenders to abort in a container and
  // abort them outside of the map iteration. This is because that
  // Appender::abort() can possibly complete/delete Appenders in the
  // map and it is unsafe to unlink Appenders while in the middle of
  // the intrusive map iteration.
  for (Appender* appender : appenders_to_abort) {
    ld_debug("Aborting Appender %s for log %lu.",
             lsn_to_string(appender->getLSN()).c_str(),
             appender->getLogID().val_);

    appender->abort();
    WORKER_STAT_INCR(appender_aborted_epoch);
  }

  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
