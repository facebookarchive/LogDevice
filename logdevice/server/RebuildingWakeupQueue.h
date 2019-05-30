/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <queue>
#include <set>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * RebuildingWakeupQueue is used to schedule waking up logs of a shard as we
 * make progress and the local timestamp window is slid. Waking up a log means
 * either creating and starting a new LogRebuilding state machine if it does not
 * already exist, or notifying the existing LogRebuilding state machine that the
 * local timestamp window was slid so that it can rebuild more records.
 *
 * Internally, this class maintains a timestamp, "the local window end". We
 * should not rebuild any record whose timestamp is past that value. Each log is
 * associated with its own timestamp, "the next timestamp", which is the
 * timestamp of the next record to be rebuilt for the log. A log must not be
 * woken up until the local window end is past its next timestamp.
 *
 * push() is used to schedule more logs and pop() is used to retrieve some logs
 * that can be woken up because the local window end is past their next
 * timestamp.
 *
 * All logs are first stored inside a priority queue ordered by timestamp called
 * `outsideWindowQueue_`. This queue is ordered by timestamp so that we can
 * efficiently retrieve the logs that enter the window once it is moved forward.
 *
 * Calls to advanceWindow() move logs that enter the window from
 * outsideWindowQueue_ to insideWindowQueue_, where they are to be picked up by
 * calls to pop(). `insideWindowQueue_` is ordered by log id so that we can try
 * to schedule logs with ids close to each other, which is good for read
 * locality.
 */

class RebuildingWakeupQueue {
 public:
  struct LogState {
    logid_t logId;
    RecordTimestamp nextTimestamp;

    struct TimestampComparer {
      bool operator()(const LogState& a, const LogState& b) {
        return std::tie(a.nextTimestamp, a.logId) <
            std::tie(b.nextTimestamp, b.logId);
      }
    };

    struct LogIdComparer {
      bool operator()(const LogState& a, const LogState& b) {
        return a.logId < b.logId;
      }
    };
  };

  /**
   * Clear the queue and reset the window to the minimum value.
   */
  void reset();

  /**
   * Advance the timestamp window.
   *
   * @param local_window_end new window. This function asserts that the current
   * local window end is smaller or equal than this value.
   */
  void advanceWindow(RecordTimestamp local_window_end);

  /**
   * Schedule a log.
   * The scheduled log must not already belong to this queue.
   *
   * @param logid          Id of the log to be scheduled
   * @param next_timestamp Only wake up this log once next_timestamp is before
   *                       the end of the window.
   */
  void push(logid_t logid, RecordTimestamp next_timestamp);

  /**
   * Wake up some logs.
   *
   * @param n_logs           Maximum number of logs to wake up.
   *
   * @return a vector of logs that were woken up.
   */
  std::vector<logid_t> pop(size_t n_logs);

  /**
   *
   * Removes the given logs from both insideWindowQueue_
   * and outsideWindowQueue_
   *
   * @param logs_to_remove  A set of logs to be removed
   */
  void remove(std::unordered_set<logid_t, logid_t::Hash> logs_to_remove);

  /**
   * @return the number of logs scheduled.
   */
  size_t size() const {
    return sizeInsideWindow() + sizeOutsideWindow();
  }

  /**
   * @return true if there is no log scheduled, false otherwise.
   */
  bool empty() const {
    return size() == 0;
  }

  /**
   * @return the number of logs that are scheduled and can be woken up because
   * their next timestamp is within the current window.
   */
  size_t sizeInsideWindow() const {
    return insideWindowQueue_.size();
  }

  /**
   * @return the number of logs that are scheduled but cannot be woken up
   * because their next timestamp is past the current window.
   */
  size_t sizeOutsideWindow() const {
    return outsideWindowQueue_.size();
  }

  /**
   * @return the minimum next timestamp of all logs whose next timestamp is past
   * the current window.
   * This function asserts that they are scheduled logs that are not within the
   * window (sizeOutsideWindow() > 0).
   */
  RecordTimestamp nextTimestamp() const;

 private:
  RecordTimestamp localWindowEnd_{RecordTimestamp::min()};

// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Can not remove here due to the defined variable
#ifndef NDEBUG
  // Set of logs currently in either outsideWindowQueue_ or insideWindowQueue_.
  // Used for asserts.
  std::unordered_set<logid_t, logid_t::Hash> logsInQueue_;
#endif

  // set of logs whose next timestamp is past the local window end.
  std::set<LogState, LogState::TimestampComparer> outsideWindowQueue_;

  // set of logs whose next timestamp is in the window.
  std::set<LogState, LogState::LogIdComparer> insideWindowQueue_;
};

}} // namespace facebook::logdevice
