/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Nodes.h"

#include <folly/Conv.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/lib/ClientImpl.h"

#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"

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
  auto config = client_impl->getConfig()->get();

  const auto& nodes = config->serverConfig()->getNodes();
  const auto& metadata_nodes = config->serverConfig()->getMetaDataNodeIndices();

  for (const auto& it : nodes) {
    node_index_t nid = it.first;
    const Configuration::Node& node = it.second;
    result->cols["node_id"].push_back(s(nid));
    result->cols["address"].push_back(node.address.toString());
    if (node.ssl_address.hasValue()) {
      result->cols["ssl_address"].push_back(
          node.ssl_address.value().toString());
    }
    result->cols["generation"].push_back(s(node.generation));
    if (node.location.hasValue()) {
      result->cols["location"].push_back(node.location.value().toString());
    }
    result->cols["sequencer"].push_back(
        s(node.hasRole(Configuration::NodeRole::SEQUENCER)));
    result->cols["storage"].push_back(
        s(node.hasRole(Configuration::NodeRole::STORAGE)));
    result->cols["sequencer_weight"].push_back(
        node.hasRole(Configuration::NodeRole::SEQUENCER)
            ? s(node.getSequencerWeight())
            : "");
    if (node.hasRole(Configuration::NodeRole::STORAGE)) {
      auto* storage = node.storage_attributes.get();
      result->cols["storage_state"].push_back(
          configuration::storageStateToString(node.getStorageState()));
      result->cols["storage_weight"].push_back(s(storage->capacity));
      result->cols["num_shards"].push_back(s(storage->num_shards));
    }
    const bool is_metadata_node =
        std::find(metadata_nodes.begin(), metadata_nodes.end(), nid) !=
        metadata_nodes.end();
    result->cols["is_metadata_node"].push_back(s(is_metadata_node));
  }

  return result;
}

}}}} // namespace facebook::logdevice::ldquery::tables
