/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/MetadataNodesetSelector.h"

#include <queue>
#include <tuple>
#include <utility>

#include "logdevice/common/configuration/nodes/utils.h"

namespace facebook { namespace logdevice {

// The percent of Shards that should be writable on a node
// that is in the current metadata nodeset for it be considered
// in the new nodeset
// TODO: This is not safe if all nodes in metadata nodeset have
// the same shards as unwriteable. - T48762313
constexpr double NUM_WRITEABLE_SHARDS_PER_NODE_RATIO = 0.8;

folly::Expected<std::set<node_index_t>, Status>
MetadataNodeSetSelector::getNodeSet(
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_config,
    const std::unordered_set<node_index_t>& exclude_nodes) {
  ld_check(nodes_config);
  auto replication_property =
      nodes_config->getMetaDataLogsReplication()->getReplicationProperty();
  auto replication_factors =
      replication_property.getDistinctReplicationFactors();

  ld_check(!replication_factors.empty());

  auto target_nodeset_size =
      std::min(static_cast<int>(nodes_config->getStorageNodes().size()),
               2 * replication_property.getReplicationFactor() - 1);

  NodeLocationScope replication_scope;
  if (replication_factors[0].first <= NodeLocationScope::NODE) {
    ld_warning("Metadata nodeset is not configured to have cross domain "
               "replication. This might be risky because the failure of a "
               "single domain can make the metadata unaccessible.");
    // No cross-domain replication
    replication_scope = NodeLocationScope::ROOT;
  } else {
    replication_scope = replication_factors[0].first;
  }

  struct Domain {
    std::vector<node_index_t> existing_nodes;
    std::vector<node_index_t> new_nodes;
    std::string location;
    std::string region;
    int num_picked = 0;
  };
  std::map<std::string, Domain> domains;

  const auto& membership = nodes_config->getStorageMembership();

  const auto current_nodeset = membership->getMetaDataNodeSet();

  ld_check(nodes_config);
  for (const auto node : membership->getMembershipNodes()) {
    if (exclude_nodes.count(node)) {
      ld_spew("Not adding Node N%hd to domains becasue it was requested to be "
              "excluded",
              node);
      continue;
    }
    const auto* sd = nodes_config->getNodeServiceDiscovery(node);
    ld_check(sd != nullptr);

    std::string location_str;
    std::string region;
    if (replication_scope == NodeLocationScope::ROOT) {
      // All nodes are in the same replication domain.
    } else {
      if (!sd->location.hasValue()) {
        ld_error("Can't select nodeset because node %d (%s) does not have "
                 "location information",
                 node,
                 sd->address.toString().c_str());
        return folly::makeUnexpected<Status>(E::FAILED);
      }

      const NodeLocation& location = sd->location.value();
      assert(!location.isEmpty());
      if (!location.scopeSpecified(replication_scope)) {
        ld_error("Can't select nodeset because location %s of node %d (%s) "
                 "doesn't have location for scope %s.",
                 location.toString().c_str(),
                 node,
                 sd->address.toString().c_str(),
                 NodeLocation::scopeNames()[replication_scope].c_str());
        return folly::makeUnexpected<Status>(E::FAILED);
      }
      location_str = location.getDomain(replication_scope, node);
      region = location.getDomain(NodeLocationScope::REGION, node);
    }

    auto shard_states =
        nodes_config->getStorageMembership()->getShardStates(node);
    auto num_shards = shard_states.size();
    bool is_node_in_current_nodeset = current_nodeset.count(node);
    int num_writeable_shards = 0;

    for (const auto& kv : shard_states) {
      ShardID shard = ShardID(node, kv.first);
      if (membership->canWriteToShard(shard)) {
        num_writeable_shards++;
      }
      // If a node is in the current nodeset, then all its shards
      // should be part of MetadataStorageState in membership
      if (is_node_in_current_nodeset) {
        ld_check(membership->isInMetadataStorageSet(shard));
      }
    }

    if (is_node_in_current_nodeset) {
      // We will consider keep the existing node only if the number of
      // shards whose StorageState is READ_WRITE is above a threshold
      if (num_writeable_shards >=
          (NUM_WRITEABLE_SHARDS_PER_NODE_RATIO * num_shards)) {
        domains[location_str].existing_nodes.push_back(node);
        domains[location_str].region = region;
        domains[location_str].location = location_str;
      }
    } else {
      // We will only consider new nodes which have all shards in READ_WRITE
      if (num_writeable_shards == num_shards) {
        domains[location_str].new_nodes.push_back(node);
        domains[location_str].region = region;
        domains[location_str].location = location_str;
      }
    }
  }

  // Keep track of number of nodes picked per region
  std::unordered_map<std::string, int> num_nodes_picked_in_region;
  // Keep track of number of unique domains in largest scope picked
  int num_domains_picked = 0;

  auto comparator = [&](Domain* a, Domain* b) {
    // Domains with existing nodes have high priority
    auto a_has_existing_nodes = a->existing_nodes.size() > 0 ? 0 : 1;
    auto b_has_existing_nodes = b->existing_nodes.size() > 0 ? 0 : 1;
    return std::tie(num_nodes_picked_in_region[a->region],
                    a->num_picked,
                    a_has_existing_nodes,
                    a->location) >
        std::tie(num_nodes_picked_in_region[b->region],
                 b->num_picked,
                 b_has_existing_nodes,
                 b->location);
  };
  std::priority_queue<Domain*, std::vector<Domain*>, decltype(comparator)>
      queue(comparator);

  for (auto& kv : domains) {
    Domain* d = &kv.second;
    ld_check(!d->existing_nodes.empty() || !d->new_nodes.empty());
    queue.push(d);
  }

  std::set<node_index_t> new_nodeset;

  while (!queue.empty()) {
    Domain* d = queue.top();
    queue.pop();
    ld_check(!d->existing_nodes.empty() || !d->new_nodes.empty());
    if (new_nodeset.size() >= target_nodeset_size) {
      // We have selected enough nodes in the nodeset
      break;
    }
    if (!d->existing_nodes.empty()) {
      new_nodeset.insert(d->existing_nodes.back());
      d->existing_nodes.pop_back();
    } else {
      ld_check(!d->new_nodes.empty());
      new_nodeset.insert(d->new_nodes.back());
      d->new_nodes.pop_back();
    }
    d->num_picked++;
    num_nodes_picked_in_region[d->region]++;
    if (d->num_picked == 1) {
      num_domains_picked += 1;
    }
    if (!d->existing_nodes.empty() || !d->new_nodes.empty()) {
      queue.push(d);
    }
  }

  {
    // Convert it to StorageSet to check validity
    StorageSet storage_set;
    for (auto node : new_nodeset) {
      auto shards = membership->getShardStates(node);
      for (const auto& [shard, _] : shards) {
        storage_set.push_back(ShardID(node, shard));
      }
    }
    if (!configuration::nodes::validStorageSet(
            *nodes_config, storage_set, replication_property)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      5,
                      "Not enough storage nodes to select a metadata nodeset "
                      "replication: %s, selected: %s.",
                      toString(replication_property).c_str(),
                      toString(new_nodeset).c_str());
      return folly::makeUnexpected<Status>(E::FAILED);
    }
  }

  // Check that nodeset has enough racks and regions. Require at least one
  // extra domain in the largest scope in the nodeset to tolerate loss of
  // single domain
  if (replication_factors[0].first > NodeLocationScope::NODE &&
      replication_factors[0].second > 1 &&
      num_domains_picked <= replication_factors[0].second) {
    ld_warning("Not enough nodes to generate a nodeset with an extra domain in "
               "the largest scope. This generated nodeset won't be able to "
               "tolerate the loss of a single domain."
               "Replication Property:%s, "
               "Num domains picked:%s, "
               "Num domains required:>%s , Selected nodes:%s",
               toString(replication_property).c_str(),
               toString(num_domains_picked).c_str(),
               toString(replication_factors[0].second).c_str(),
               toString(new_nodeset).c_str());
  }

  return new_nodeset;
}

}} // namespace facebook::logdevice
