/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"

namespace facebook { namespace logdevice { namespace maintenance {

class MaintenanceManager;

/**
 * A simple wrapper to write an event to EventLog.
 */

class EventLogWriter {
 public:
  explicit EventLogWriter(EventLogStateMachine& event_log)
      : event_log_(event_log) {}

  /*
   * calls writeDelta on EventLogStateMachine
   *
   * @param  event The EventLogRecord that need to be written
   * @param  cb  The callback that gets called once write to event log
   *             is complete. Note: This callback will be called in
   *             context of the thread which does the write.
   */

  void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<
          void(Status st, lsn_t version, const std::string& /* unused */)> cb)
      const;

 private:
  EventLogStateMachine& event_log_;
};
}}} // namespace facebook::logdevice::maintenance
