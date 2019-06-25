/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/Nodes.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/utils.h"
#include "logdevice/lib/ClientImpl.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns Nodes::getColumns() const {
  return {{"node_id", DataType::BIGINT, "Id of the node"},
          {"address",
           DataType::TEXT,
           "Ip and port that should be used for communication with the node"},
          {"ssl_address", DataType::TEXT, "Same as \"address\" but with SSL"},
          {"generation",
           DataType::BIGINT,
           "Generation of the node.  This value is bumped each time the "
           "node is swapped, sent to repair, or has one of its drives "
           "sent to repair."},
          {"location",
           DataType::TEXT,
           "Location of the node: <region>.<cluster>.<row>.<rack>"},
          {"sequencer",
           DataType::INTEGER,
           "1 if this node is provisioned for the sequencing role. "
           "Otherwise 0. Provisioned roles must be enabled in order "
           "to be considered active. See 'sequencer_enabled'."},
          {"storage",
           DataType::INTEGER,
           "1 if this node is provisioned for the storage role. "
           "Otherwise 0. Provisioned roles must be enabled in order "
           "to be considered active. See 'storage_state'."},
          {"sequencer_enabled",
           DataType::INTEGER,
           "1 if sequencing on this node is enabled. Othewise 0."},
          {"sequencer_weight",
           DataType::REAL,
           "A non-negative value indicating how many logs this node "
           "should be a sequencer for relative to other nodes in the "
           "cluster.  A value of 0 means this node cannot run "
           "sequencers."},
          {"is_storage",
           DataType::INTEGER,
           "1 if this node is provisioned for the storage role. "
           "Otherwise 0. Provisioned roles must be enabled in order "
           "to be considered active. See 'storage_state'."},
          {"storage_state",
           DataType::TEXT,
           "Determines the current state of the storage node. One "
           "of \"read-write\", \"read-only\" or \"none\"."},
          {"storage_weight",
           DataType::REAL,
           "A positive value indicating how much STORE traffic this "
           "storage node should receive relative to other storage nodes "
           "in the cluster."},
          {"num_shards",
           DataType::BIGINT,
           "Number of storage shards on this node.  "
           "0 if this node is not a storage node."},
          {"is_metadata_node",
           DataType::INTEGER,
           "1 if this node is in the metadata nodeset. Otherwise 0."}};
}

std::shared_ptr<TableData> Nodes::getData(QueryContext& /*ctx*/) {
  auto result = std::make_shared<TableData>();

  auto ld_client = ld_ctx_->getClient();
  ld_check(ld_client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(ld_client.get());
  auto config = client_impl->getConfig();

  const auto& nodes_configuration = config->getNodesConfiguration();
  const auto& seq_membership = nodes_configuration->getSequencerMembership();
  const auto& storage_membership = nodes_configuration->getStorageMembership();
  const auto& metadata_nodes = storage_membership->getMetaDataNodeIndices();

  for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
    result->newRow();
    node_index_t nid = kv.first;
    const auto& node_sd = kv.second;
    result->set("node_id", s(nid));
    result->set("address", node_sd.address.toString());
    if (node_sd.ssl_address.hasValue()) {
      result->set("ssl_address", node_sd.ssl_address.value().toString());
    }
    result->set("generation", s(nodes_configuration->getNodeGeneration(nid)));
    if (node_sd.location.hasValue()) {
      result->set("location", node_sd.location.value().toString());
    }
    result->set("sequencer",
                s(node_sd.hasRole(configuration::nodes::NodeRole::SEQUENCER)));
    result->set(
        "storage", s(node_sd.hasRole(configuration::nodes::NodeRole::STORAGE)));

    if (seq_membership->hasNode(nid)) {
      ld_check(seq_membership->getNodeStatePtr(nid) != nullptr);
      result->set(
          "sequencer_weight",
          s(seq_membership->getNodeStatePtr(nid)->getEffectiveWeight()));
    }

    if (storage_membership->hasNode(nid)) {
      const auto* storage_attr =
          nodes_configuration->getNodeStorageAttribute(nid);
      ld_check(storage_attr != nullptr);
      // TODO: support showing per-shard storage state
      // TODO: in compatibility mode, use the storage state of ShardID(nid, 0)
      ShardID compatibility_shard(nid, 0);
      auto state_res = storage_membership->getShardState(compatibility_shard);
      if (state_res.hasValue()) {
        // TODO: use the new storage state string
        // result->set(
        //     "storage_state",
        //     membership::toString(state_res->storage_state).toString());
        const auto legacy_storage_state =
            configuration::nodes::NodesConfigLegacyConverter::
                toLegacyStorageState(state_res->storage_state);
        result->set("storage_state",
                    configuration::storageStateToString(legacy_storage_state));
      } else {
        ld_warning("Node %hu does not have shard %s in storage membership!",
                   nid,
                   compatibility_shard.toString().c_str());
      }

      result->set("storage_weight", s(storage_attr->capacity));
      result->set("num_shards", s(storage_attr->num_shards));
    }

    const bool is_metadata_node =
        std::find(metadata_nodes.begin(), metadata_nodes.end(), nid) !=
        metadata_nodes.end();
    result->set("is_metadata_node", s(is_metadata_node));
  }

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
