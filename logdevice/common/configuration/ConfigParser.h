/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/NodesConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/TrafficShapingConfig.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"

namespace facebook { namespace logdevice { namespace configuration {
class LocalLogsConfig;
}}} // namespace facebook::logdevice::configuration

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

bool parseMetaDataLog(const folly::dynamic& clusterMap,
                      const SecurityConfig& securityConfig,
                      MetaDataLogsConfig& output);

bool parseTraceLogger(const folly::dynamic& clusterMap,
                      TraceLoggerConfig& output);

bool parseInternalLogs(const folly::dynamic& clusterMap,
                       configuration::InternalLogs& internalLogs);

bool validateNodeCount(const ServerConfig& nodes_cfg,
                       const LocalLogsConfig* logs_cfg_ptr);

/**
 * Given the replication factor (the only required field in metadata logs
 * config), synced_copies and its synchonous replication scope, return a
 * Log object with default configuration for metadata logs.
 */
Log genDefaultMetaDataLogsConfig(int replication_factor,
                                 int syncd_copies,
                                 NodeLocationScope sync_replication_scope);

}}}} // namespace facebook::logdevice::configuration::parser
