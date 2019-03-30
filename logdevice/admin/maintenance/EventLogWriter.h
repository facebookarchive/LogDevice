/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/common/event_log/EventLogRecord.h"

namespace facebook { namespace logdevice { namespace maintenance {

class MaintenanceManager;

class EventLogWriter {
 public:
  EventLogWriter(const MaintenanceManager* owner) : owner_(owner) {}

  /*
   * Adds work to the MaintenanceManager work context to
   * write an event to the event log
   *
   * @param  event The EventLogRecord that need to be written
   * @param  cb  The callback that gets called once write to event log
   *             is complete. Note: This callback will be called in
   *             MaintenanceManager's context since it the entity performing
   *             the write
   */

  void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<
          void(Status st, lsn_t version, const std::string& /* unused */)> cb)
      const;

 private:
  const MaintenanceManager* owner_;
};
}}} // namespace facebook::logdevice::maintenance
