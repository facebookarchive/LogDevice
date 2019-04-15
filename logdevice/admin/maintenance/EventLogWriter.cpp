/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/EventLogWriter.h"

#include "logdevice/admin/maintenance/MaintenanceManager.h"

namespace facebook { namespace logdevice { namespace maintenance {

void EventLogWriter::writeToEventLog(
    std::unique_ptr<EventLogRecord> event,
    std::function<
        void(Status st, lsn_t version, const std::string& /* unused */)> cb)
    const {
  ld_check(event != nullptr);
  event_log_.writeDelta(*event, cb);
}

}}} // namespace facebook::logdevice::maintenance
