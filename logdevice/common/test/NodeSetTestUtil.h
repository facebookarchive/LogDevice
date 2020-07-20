/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Optional.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice { namespace NodeSetTestUtil {

// add num_nodes in the nodes map with given attributes
void addNodes(
    std::shared_ptr<const NodesConfiguration>& nodes,
    size_t num_nodes,
    shard_size_t num_shards,
    std::string location_string = "",
    double weight = 1.,
    double sequencer = 1.,
    membership::StorageState state = membership::StorageState::READ_WRITE,
    bool metadata_node = false);

// add a log to the logs_config with given attributes
void addLog(configuration::LocalLogsConfig* logs_config,
            logid_t logid,
            ReplicationProperty replication,
            int extras,
            size_t nodeset_size,
            folly::Optional<std::chrono::seconds> backlog = folly::none);

inline void
addLog(configuration::LocalLogsConfig* logs_config,
       logid_t logid,
       int replication,
       int extras,
       size_t nodeset_size,
       folly::Optional<std::chrono::seconds> backlog,
       NodeLocationScope sync_replication_scope = NodeLocationScope::NODE) {
  addLog(logs_config,
         logid,
         ReplicationProperty(replication, sync_replication_scope),
         extras,
         nodeset_size,
         backlog);
}

}}} // namespace facebook::logdevice::NodeSetTestUtil
