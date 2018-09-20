/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "NodeSetTestUtil.h"

#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/String.h>
#include "logdevice/common/util.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"

namespace facebook { namespace logdevice { namespace NodeSetTestUtil {

void addNodes(ServerConfig::Nodes* nodes,
              size_t num_nodes,
              shard_size_t num_shards,
              folly::Optional<std::chrono::seconds> retention,
              std::string location_string,
              size_t num_non_zw_nodes,
              double weight,
              double sequencer) {
  ld_check(nodes != nullptr);
  ld_check(num_nodes >= num_non_zw_nodes);

  node_index_t first_new_index = 0;
  for (const auto& it : *nodes) {
    first_new_index = std::max(first_new_index, (node_index_t)(it.first + 1));
  }

  std::vector<ServerConfig::Node> new_nodes;
  for (size_t i = 0; i < num_nodes; ++i) {
    ServerConfig::Node node;
    node.address = Sockaddr("::1", std::to_string(first_new_index + i));
    node.storage_capacity = weight;
    node.storage_state = (i < num_non_zw_nodes)
        ? configuration::StorageState::READ_WRITE
        : configuration::StorageState::READ_ONLY;
    node.sequencer_weight = sequencer;
    node.num_shards = num_shards;
    node.generation = 1;
    node.retention = retention;
    if (!location_string.empty()) {
      NodeLocation loc;
      int rv = loc.fromDomainString(location_string);
      ld_check(rv == 0);
      node.location = std::move(loc);
    }
    new_nodes.push_back(node);
  }

  const size_t size_begin = nodes->size();
  // shuffle the nodes added
  std::shuffle(new_nodes.begin(), new_nodes.end(), folly::ThreadLocalPRNG());
  for (size_t i = 0; i < new_nodes.size(); ++i) {
    (*nodes)[first_new_index + i] = new_nodes[i];
  }

  ld_check(nodes->size() == size_begin + num_nodes);
}

void addLog(configuration::LocalLogsConfig* logs_config,
            logid_t logid,
            ReplicationProperty replication,
            int extras,
            size_t nodeset_size,
            folly::Optional<std::chrono::seconds> backlog) {
  // log must not already exist
  ld_check(logs_config->getLogMap().find(logid.val_) ==
           logs_config->getLogMap().end());
  LogsConfig::Log log;
  log.maxWritesInFlight = 256;
  log.replicationFactor = replication.getReplicationFactor();
  log.rangeName = folly::to<std::string>(logid.val());
  log.extraCopies = extras;
  log.nodeSetSize = nodeset_size;
  log.backlogDuration = backlog;
  log.replicateAcross = replication.getDistinctReplicationFactors();
  boost::icl::right_open_interval<logid_t::raw_type> logid_interval(
      logid.val_, logid.val_ + 1);
  logs_config->insert(logid_interval, log);
}

}}} // namespace facebook::logdevice::NodeSetTestUtil
