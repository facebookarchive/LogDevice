/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/NodeSetTestUtil.h"

#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/String.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice { namespace NodeSetTestUtil {

void addNodes(std::shared_ptr<const NodesConfiguration>& nodes,
              size_t num_nodes,
              shard_size_t num_shards,
              std::string location_string,
              double weight,
              double sequencer,
              membership::StorageState state,
              bool metadata_node) {
  ld_check(nodes != nullptr);

  std::vector<ShardID> added_shards;

  configuration::Nodes new_nodes;
  node_index_t idx =
      nodes->clusterSize() > 0 ? nodes->getMaxNodeIndex() + 1 : 0;
  for (size_t i = 0; i < num_nodes; ++i) {
    new_nodes.emplace(idx,
                      configuration::Node::withTestDefaults(idx)
                          .setLocation(location_string)
                          .addSequencerRole(true, sequencer)
                          .addStorageRole(num_shards, weight)
                          .setIsMetadataNode(metadata_node));
    added_shards.emplace_back(idx, -1);
    idx++;
  }

  nodes = nodes->applyUpdate(NodesConfigurationTestUtil::addNewNodesUpdate(
      *nodes, std::move(new_nodes)));
  ld_check(nodes);

  nodes =
      nodes->applyUpdate(NodesConfigurationTestUtil::setStorageMembershipUpdate(
          *nodes, added_shards, state, folly::none));
  ld_check(nodes);
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
  auto log_attrs =
      logsconfig::LogAttributes()
          .with_maxWritesInFlight(256)
          .with_replicationFactor(replication.getReplicationFactor())
          .with_extraCopies(extras)
          .with_nodeSetSize(nodeset_size)
          .with_backlogDuration(backlog)
          .with_replicateAcross(replication.getDistinctReplicationFactors());
  boost::icl::right_open_interval<logid_t::raw_type> logid_interval(
      logid.val_, logid.val_ + 1);
  logs_config->insert(
      logid_interval, folly::to<std::string>(logid.val()), log_attrs);
}
}}} // namespace facebook::logdevice::NodeSetTestUtil
