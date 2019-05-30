/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingWakeupQueue.h"

#include <unordered_map>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

void RebuildingWakeupQueue::reset() {
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Do not replace now since logsInQueue_ is defined within NDEBUG
#ifndef NDEBUG
  logsInQueue_.clear();
#endif
  outsideWindowQueue_ = decltype(outsideWindowQueue_)();
  insideWindowQueue_ = decltype(insideWindowQueue_)();
  localWindowEnd_ = RecordTimestamp::min();
}

void RebuildingWakeupQueue::advanceWindow(RecordTimestamp local_window_end) {
  ld_check(local_window_end >= localWindowEnd_);
  localWindowEnd_ = std::max(localWindowEnd_, local_window_end);

  // Find all logs inside `outsideWindowQueue_` that can be moved to
  // `insideWindowQueue_` because their nextTimestamp now falls inside the
  // window.
  while (!outsideWindowQueue_.empty()) {
    const LogState& log = *(outsideWindowQueue_.begin());
    if (log.nextTimestamp > localWindowEnd_) {
      break;
    }

    insideWindowQueue_.insert(log);
    outsideWindowQueue_.erase(outsideWindowQueue_.begin());
  }
}

void RebuildingWakeupQueue::push(logid_t logid,
                                 RecordTimestamp next_timestamp) {
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Do not replace now since logsInQueue_ is defined within NDEBUG
#ifndef NDEBUG
  ld_assert(logsInQueue_.find(logid) == logsInQueue_.end());
  logsInQueue_.insert(logid);
#endif

  LogState log;
  log.logId = logid;
  log.nextTimestamp = next_timestamp;

  if (log.nextTimestamp <= localWindowEnd_) {
    insideWindowQueue_.insert(log);
  } else {
    outsideWindowQueue_.insert(log);
  }
}

std::vector<logid_t> RebuildingWakeupQueue::pop(size_t n_logs) {
  std::vector<logid_t> res;

  while (!insideWindowQueue_.empty() && res.size() < n_logs) {
    auto it = insideWindowQueue_.begin();
    ld_check(it->nextTimestamp <= localWindowEnd_);
    res.push_back(it->logId);
    insideWindowQueue_.erase(it);
  }

// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Do not replace now since logsInQueue_ is defined within NDEBUG
#ifndef NDEBUG
  for (logid_t logid : res) {
    ld_assert(logsInQueue_.find(logid) != logsInQueue_.end());
    logsInQueue_.erase(logid);
  }
#endif

  return res;
}

void RebuildingWakeupQueue::remove(
    std::unordered_set<logid_t, logid_t::Hash> logs_to_remove) {
  // Remove the logs from both the queues
  auto insideIterator = insideWindowQueue_.begin();
  while (insideIterator != insideWindowQueue_.end() &&
         !logs_to_remove.empty()) {
    if (logs_to_remove.count((*insideIterator).logId)) {
      logs_to_remove.erase(insideIterator->logId);
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Do not replace now since logsInQueue_ is defined within NDEBUG
#ifndef NDEBUG
      ld_assert(logsInQueue_.count(insideIterator->logId));
      logsInQueue_.erase(insideIterator->logId);
#endif
      insideIterator = insideWindowQueue_.erase(insideIterator);
    } else {
      insideIterator++;
    }
  }

  auto outsideIterator = outsideWindowQueue_.begin();
  while (outsideIterator != outsideWindowQueue_.end() &&
         !logs_to_remove.empty()) {
    if (logs_to_remove.count((*outsideIterator).logId)) {
      logs_to_remove.erase(outsideIterator->logId);
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Do not replace now since logsInQueue_ is defined within NDEBUG
#ifndef NDEBUG
      ld_assert(logsInQueue_.count(outsideIterator->logId));
      logsInQueue_.erase(outsideIterator->logId);
#endif
      outsideIterator = outsideWindowQueue_.erase(outsideIterator);
    } else {
      outsideIterator++;
    }
  }
}

RecordTimestamp RebuildingWakeupQueue::nextTimestamp() const {
  ld_check(!outsideWindowQueue_.empty());
  return outsideWindowQueue_.begin()->nextTimestamp;
}

}} // namespace facebook::logdevice
