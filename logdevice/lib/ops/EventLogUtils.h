/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace EventLogUtils {

using PrintEventCallback =
    std::function<void(const DataRecord&, EventLogRecord&)>;

/**
 * Tail event log, calling `cb` after every EventLogRebuildingSet update.
 * Stop when one of:
 *  - `cb` returns false,
 *  - `stop_at_tail` is true, and we reached the tail LSN of event log,
 *  - `stop_on_signal` is true, and we got a SIGTERM or SIGINT.
 *
 * @param client Client to use to read the event log
 * @param set    Set to be filled with the final state after all tailing is
 *               done. Can be nullptr.
 * @param cb     Callback to call after each update. Return true to continue
 *               reading, false to stop. Stopping is not immediate: we may
 *               process a few more updates (and call `cb` for them) after
 *               `cb` returns false.
 * @param timeout  Give up and return -1 after this many milliseconds.
 * @param stop_at_tail  If true, fetch tail LSN before reading and only read up
 *                      to that LSN.
 * @param stop_on_signal   If true, register for the SIGTERM and SIGINT signals
 *                         and stop reading until the process receives SIGTERM
 *                         or SIGINT.
 *
 * @return 0 on success or -1 and err is set to:
 *         - E::TIMEDOUT if no stopping condition was reached within `timeout`
 */
int tailEventLog(
    Client& client,
    EventLogRebuildingSet* set,
    std::function<bool(const EventLogRebuildingSet& set,
                       const EventLogRecord*,
                       lsn_t)> cb,
    std::chrono::milliseconds timeout = std::chrono::milliseconds::max(),
    bool stop_at_tail = false,
    bool stop_on_signal = false);

/**
 * Retrieve a EventLogRebuildingSet by reading the event log up to current tail.
 * See tailEventLog() for description of parameters and return value.
 * `client`'s timeout is used as timeout for reading the event log.
 */
int getRebuildingSet(Client& client, EventLogRebuildingSet& set);

/**
 * @param client     Client to use to read the event log
 * @param map        Map to be filled with data read from the event log
 *
 * @return @see getRebuildingSet().
 */
int getShardAuthoritativeStatusMap(Client& client,
                                   ShardAuthoritativeStatusMap& map);

/**
 * Trim the event log RSM.
 *
 * @param client    Client to use to read/trim/findtime the event log RSM.
 * @param timestamp Trim all entries older than this timestamp.
 *
 * @return E::OK on success or any error that can be returned by findTime, trim.
 */
Status trim(Client& client, std::chrono::milliseconds timestamp);

}}} // namespace facebook::logdevice::EventLogUtils
