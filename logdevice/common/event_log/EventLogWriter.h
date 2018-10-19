/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <queue>
#include <string>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

/**
 * @file EventLogWriter provides API to write events to the EventLog
 */

namespace facebook { namespace logdevice {

class BackoffTimer;
class EventLogRecord;
class Processor;
template <class T>
class WorkerCallbackHelper;

class EventLogWriter {
 public:
  explicit EventLogWriter(EventLogStateMachine* event_log);

  virtual ~EventLogWriter();

  /**
   * Asynchronously write an event to the event log.
   *
   * @param event Event to be written;
   */
  void writeEvent(std::unique_ptr<EventLogRecord> event);

 protected:
  virtual std::unique_ptr<BackoffTimer>
  createAppendRetryTimer(std::function<void()> callback);

 private:
  EventLogStateMachine* event_log_;

  /**
   * Queue of events waiting to be appended to the event log.  The front of the
   * queue is either an append in flight or an waiting to be appended
   * after the `appendRequestRetryTimer_` triggers.
   */
  std::queue<std::unique_ptr<EventLogRecord>> appendQueue_;

  /**
   * Timer with exponential backoff used for retrying appending a message to the
   * event log.
   */
  std::unique_ptr<BackoffTimer> appendRequestRetryTimer_;

  /**
   * Used to get a callback on this worker thread when an AppendRequest to write
   * to the event log completes.
   */
  std::unique_ptr<WorkerCallbackHelper<EventLogWriter>> callbackHelper_;

  /**
   * Write the next serialized event in `appendQueue_` to the event log.
   */
  void writeNextEventInQueue();
};

}} // namespace facebook::logdevice
