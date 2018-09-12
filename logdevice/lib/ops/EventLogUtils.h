/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"

#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace EventLogUtils {

using PrintEventCallback =
    std::function<void(const DataRecord&, EventLogRecord&)>;

/**
 * Retrieve a EventLogRebuildingSet by reading the event log.
 * @param client Client to use to read the event log
 * @param set    Set to be filled with data read from the event log
 * @param tail   If true, register for the SIGTERM and SIGINT signals and read
 *               the event log forever until the process receives SIGTERM or
 *               SIGINT. If false, this function guarantees that it will return
 *               after it read all records appended to the event log prior to
 *               the call being made.
 *
 * @return 0 on success or -1 and err is set to:
 *         - E::NOTFOUND if there is no event log configured in the cluster;
 *         - Any error code returned by Client::getTailLSNSync() if we could not
 *           retrieved the tail LSN of the event log;
 *         - Any error code returned by Reader::startReading() if we could not
 *           read the event log.
 */
int getRebuildingSet(Client& client,
                     EventLogRebuildingSet& set,
                     bool tail = false);

/**
 * @param client     Client to use to read the event log
 * @param map        Map to be filled with data read from the event log
 *
 * @return @see getRebuildingSet().
 */
int getShardAuthoritativeStatusMap(Client& client,
                                   ShardAuthoritativeStatusMap& map);

/**
 * Check if we can wipe some shards without causing data loss.
 *
 * @param client          Client to use for reading the event log.
 * @param map             ShardAuthoritativeStatusMap retrieved with a call to
 *                        getShardAuthoritativeStatusMap for instance.
 * @param shards          List of shards we want to wipe.
 * @param min_replication Minimin replication property for the logs that may be
 *                        stored on these shards.
 *
 * @return True if wiping these shards would not cause dataloss. Returns false
 * otherwise. Note that this function returns False if there are already too
 * many nodes not in the input list that are underreplicated.
 */
bool canWipeShardsWithoutCausingDataLoss(
    Client& client,
    const ShardAuthoritativeStatusMap& map,
    std::vector<std::pair<node_index_t, uint32_t>> shards,
    ReplicationProperty min_replication);

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
