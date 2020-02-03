/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/Client.h"

namespace facebook {
  namespace logdevice {
    namespace ops {
      namespace LogsConfig {

/***
 * This dumps the current LogsConfigTree loaded by this client, serializes it
 * using FBuffersLogsConfigCodec and returns the result.
 */
std::string dump(Client& client);

/**
 * Trim the logsconfig RSM (snapshots, deltas)
 *
 * @param client    Client to use to read/trim/findtime the logsconfig RSM
 * @param timestamp Trim all entries older than this timestamp.
 * @param trim_everything If true, this means that everything will be deleted
 *                        (including the latest snapshot)
 *
 * @return E::OK on success or any error that can be returned by findTime, trim.
 */
Status trim(Client& client, std::chrono::milliseconds timestamp);

/**
 * Trims everything in the LogsConfig RSM and all the snapshots.
 *
 * @param client    Client to use to read/trim the logsconfig RSM
 *
 * @return E::OK on success or any error that can be returnd while trimming.
 */
Status trimEverything(Client& client);

/**
 * Restore the LogsConfig state to the supplied serialized copy
 */
int restoreFromDump(Client& client, std::string serialized_dump);

/**
 * Converts the supplied serialized bin copy into JSON (returned as string
 * by this function)
 */
std::string convertToJson(Client& client, std::string serialized_dump);

/**
 * Forces the LogsConfig to take a new snapshot
 */
int takeSnapshot(Client& client);

/**
 * Returns true if the LogsConfigStateMachine could return an LSN
 * (i.e both base snapshot and the delta was read) within the specified timeout
 */
bool isConfigLogReadable(Client& client);

}}}} // namespace facebook::logdevice::ops::LogsConfig
