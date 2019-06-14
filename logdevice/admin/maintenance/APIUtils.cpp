/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/APIUtils.h"

#include <folly/Random.h>
#include <folly/container/F14Set.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/admin/toString.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/util.h"

using namespace facebook::logdevice::thrift;

namespace facebook { namespace logdevice { namespace maintenance {
// Helper functions for Maintenance API requests and responses
namespace APIUtils {

folly::Optional<InvalidRequest>
validateDefinition(const MaintenanceDefinition& definition) {
  // shards and/or sequencer_nodes must be non-empty.
  if (definition.get_shards().empty() &&
      definition.get_sequencer_nodes().empty()) {
    return InvalidRequest(
        "At least one of shards or sequencer_nodes must be set");
  }
  // user must be well-formed and set.
  if (definition.get_user().empty()) {
    return InvalidRequest(
        "user must be set for the definition to non-empty string");
  }
  // does user has whitespaces?
  const std::string& user = definition.get_user();
  if (contains_whitespaces(user)) {
    return InvalidRequest("user cannot contain whitespaces");
  }

  // sequencer_target_state must be set if sequencer_nodes is non-empty
  if (!definition.get_sequencer_nodes().empty() &&
      definition.sequencer_target_state != SequencingState::DISABLED) {
    return InvalidRequest(
        "sequencer_target_state must be DISABLED if sequencer_nodes is set");
  }
  // shard_target_state must be either MAY_DISAPPEAR or DRAINED is shards is
  // non-empty
  if (!definition.get_shards().empty() &&
      !(definition.get_shard_target_state() ==
            ShardOperationalState::MAY_DISAPPEAR ||
        definition.get_shard_target_state() ==
            ShardOperationalState::DRAINED)) {
    return InvalidRequest("Only MAY_DISAPPEAR or DRAINED are acceptable values "
                          "for shard_target_state if shards is set");
  }

  // if shards is set, the shard_index has to be a positive value and/or -1.
  // smaller than -1 is not accepted value.
  for (const auto& shard : definition.get_shards()) {
    if (shard.get_shard_index() < -1) {
      return InvalidRequest("Cannot accept shard_index smaller than -1");
    }
  }

  // force_restore_rebuilding cannot be set by the user.
  if (definition.get_force_restore_rebuilding()) {
    return InvalidRequest("Setting force_restore_rebuilding is not allowed");
  }
  // ttl cannot be a negative numebr
  if (definition.get_ttl_seconds() < 0) {
    return InvalidRequest("ttl_seconds must be a non-negative number");
  }
  // group_id must NOT be set by the user
  if (definition.group_id_ref().has_value()) {
    return InvalidRequest("group_id cannot be set by the user");
  }
  // last_check_impact_result should not be set by the user
  if (definition.last_check_impact_result_ref().has_value()) {
    return InvalidRequest("last_check_impact_result cannot be set by the user");
  }
  // expires_on should not be set by the user
  if (definition.expires_on_ref().has_value()) {
    return InvalidRequest("expires_on cannot be set by the user");
  }
  // created_on should not be set by the user
  if (definition.created_on_ref().has_value()) {
    return InvalidRequest("created_on cannot be set by the user");
  }
  return folly::none;
}

folly::Expected<std::vector<MaintenanceDefinition>, InvalidRequest>
expandMaintenances(
    const MaintenanceDefinition& definition,
    const std::shared_ptr<const NodesConfiguration>& nodes_config) {
  std::vector<MaintenanceDefinition> output;
  try {
    // Step1: Expand all shards and resolve nodes to find the node_index.
    //
    // this may throw InvalidRequest.
    // It returns logdevice's unordered_set<ShardID>
    auto all_shards = expandShardSet(
        definition.get_shards(), *nodes_config, /* ignore_missing = */ false);

    folly::F14FastSet<node_index_t> all_sequencers;
    for (const auto& node : definition.get_sequencer_nodes()) {
      if (!isNodeIDSet(node)) {
        return folly::makeUnexpected(InvalidRequest(
            "A NodeID object passed in the list of sequencers is completely "
            "unset. At least a single value must be set"));
      }
      folly::Optional<node_index_t> found_node =
          findNodeIndex(node, *nodes_config);
      if (!found_node) {
        return folly::makeUnexpected(InvalidRequest(folly::sformat(
            "Sequencer node {} was not found in the nodes configuration",
            toString(node))));
      }
      // validate that it's actually a sequencer node
      if (!nodes_config->isSequencerNode(*found_node)) {
        return folly::makeUnexpected(InvalidRequest(
            folly::sformat("Node {} is not a sequencer node", *found_node)));
      }
      all_sequencers.insert(*found_node);
    }

    // Step2: if group = false, we need to group shards and sequencers together
    // into maintenances. If true, put everything in the same maintenance
    if (definition.get_group()) {
      // everything goes into one group
      // Now we need to convert all_shards into a set of thrift's ShardID object
      thrift::ShardSet maintenance_shards;
      for (const auto& shard : all_shards) {
        maintenance_shards.push_back(mkShardID(shard));
      }
      MaintenanceDefinition new_def;
      new_def.set_shards(std::move(maintenance_shards));
      // We need to add all_sequencers here too
      std::vector<thrift::NodeID> maintenance_sequencers;
      for (node_index_t node : all_sequencers) {
        thrift::NodeID target_node;
        target_node.set_node_index(node);
        maintenance_sequencers.push_back(target_node);
      }
      new_def.set_sequencer_nodes(std::move(maintenance_sequencers));
      // Adding a single maintenance
      output.push_back(std::move(new_def));
    } else {
      // group is set to false, we need to explode this maintnance into many.
      folly::F14FastMap<int32_t, MaintenanceDefinition> definitions_per_node;
      // We need to group maintenances by node and create groups accordingly.
      // For every shard, we group by node_index
      // node_index_t -> [shards]
      folly::F14FastMap<int32_t, thrift::ShardSet> by_node_shards =
          groupShardsByNode(all_shards);

      for (auto& it : by_node_shards) {
        MaintenanceDefinition def;
        def.set_shards(it.second);
        definitions_per_node[it.first] = def;
      }

      // Do we have sequencer nodes left?
      for (node_index_t node : all_sequencers) {
        // we have already added a definition for this node.
        if (definitions_per_node.count(node) > 0) {
          // Add the sequencer
          definitions_per_node[node].set_sequencer_nodes({mkNodeID(node)});
        } else {
          // We need a new maintenance for this sequencer-only maintenance.
          MaintenanceDefinition new_def;
          new_def.set_sequencer_nodes({mkNodeID(node)});
          definitions_per_node[node] = std::move(new_def);
        }
      }
      // Now, let's add all created maintenance(s) to output
      for (auto& it : definitions_per_node) {
        output.push_back(std::move(it.second));
      }
    }

    // Step3: All the user-supplied fields to the generated maintenances and
    // the system-generated fields too.
    fillSystemGeneratedAttributes(definition, output);
    return output;
  } catch (InvalidRequest& e) {
    // This can be thrown if expandShardSet failed.
    // TODO: Refactor expandShardSet to use folly::Expected instead.
    return folly::makeUnexpected(e);
  }
}

folly::F14FastMap<int32_t, thrift::ShardSet>
groupShardsByNode(const ShardSet& shards) {
  folly::F14FastMap<int32_t, thrift::ShardSet> shards_per_node;
  for (const auto& shard : shards) {
    shards_per_node[shard.node()].push_back(mkShardID(shard));
  }
  return shards_per_node;
}

void fillSystemGeneratedAttributes(
    const MaintenanceDefinition& input,
    std::vector<MaintenanceDefinition>& definitions) {
  // System-generated values.
  auto created_on = SystemTimestamp::now().toMilliseconds().count();
  int32_t expires_on = 0;
  if (input.get_ttl_seconds() > 0) {
    expires_on = SystemTimestamp::now().toMilliseconds().count() +
        input.get_ttl_seconds() * 1000;
  }

  for (auto& def : definitions) {
    // set all group-id
    def.set_group(true);
    def.set_group_id(generateGroupID(8));
    // the user supplied attributes
    def.set_user(input.get_user());
    def.set_reason(input.get_reason());
    def.set_extras(input.get_extras());
    // ensure that shard target state and sequencer targets are same for all
    def.set_shard_target_state(input.get_shard_target_state());
    def.set_sequencer_target_state(input.get_sequencer_target_state());
    def.set_skip_safety_checks(input.get_skip_safety_checks());
    def.set_allow_passive_drains(input.get_allow_passive_drains());
    def.set_force_restore_rebuilding(input.get_force_restore_rebuilding());
    def.set_ttl_seconds(input.get_ttl_seconds());
    // System-generated values.
    def.set_created_on(created_on);
    if (input.get_ttl_seconds() > 0) {
      def.set_expires_on(expires_on);
    }
  }
}

bool areMaintenancesEquivalent(const MaintenanceDefinition& def1,
                               const MaintenanceDefinition& def2) {
  // They need to be from the same user.
  if (def1.get_user() != def2.get_user()) {
    return false;
  }

  if (def1.get_shards() != def2.get_shards()) {
    return false;
  }
  if (def1.get_shard_target_state() != def2.get_shard_target_state()) {
    return false;
  }
  if (def1.get_sequencer_nodes() != def2.get_sequencer_nodes()) {
    return false;
  }
  if (def1.get_sequencer_target_state() != def2.get_sequencer_target_state()) {
    return false;
  }
  return true;
}

folly::Optional<MaintenanceDefinition>
findEquivalentMaintenance(const std::vector<MaintenanceDefinition>& defs,
                          const MaintenanceDefinition& target) {
  for (const auto& def1 : defs) {
    if (areMaintenancesEquivalent(def1, target)) {
      return def1;
    }
  }
  return folly::none;
}

folly::Optional<MaintenanceDefinition>
findMaintenanceByID(const std::string& group_id,
                    const std::vector<MaintenanceDefinition>& defs) {
  for (const auto& def : defs) {
    if (def.group_id_ref() && def.group_id_ref().value() == group_id) {
      return def;
    }
  }
  return folly::none;
}

std::string generateGroupID(size_t length) {
  std::string out;
  out.reserve(length);
  static const char alphanum[] = "0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < length; ++i) {
    char c = alphanum[folly::Random::rand32(sizeof(alphanum) - 1)];
    out.insert(out.end(), c);
  }
  return out;
}

bool isMaintenancesFilterSet(const thrift::MaintenancesFilter& filter) {
  bool is_set = false;
  is_set |= filter.user_ref().has_value();
  is_set |= !filter.get_group_ids().empty();
  return is_set;
}

std::vector<MaintenanceDefinition>
filterMaintenances(const thrift::MaintenancesFilter& filter,
                   const std::vector<MaintenanceDefinition>& maintenances) {
  std::vector<MaintenanceDefinition> out;
  matchMaintenances(
      filter, maintenances, [&](const auto& def) { out.push_back(def); });
  return out;
}

void matchMaintenances(const thrift::MaintenancesFilter& filter,
                       const std::vector<MaintenanceDefinition>& maintenances,
                       folly::Function<void(const MaintenanceDefinition&)> cb) {
  std::for_each(maintenances.begin(),
                maintenances.end(),
                [&](const MaintenanceDefinition& def) {
                  if (doesMaintenanceMatchFilter(filter, def)) {
                    cb(def);
                  }
                });
}

bool doesMaintenanceMatchFilter(const thrift::MaintenancesFilter& filter,
                                const MaintenanceDefinition& def) {
  // If user is set, we must filter by user.
  if (filter.user_ref().has_value() &&
      filter.user_ref().value() != def.get_user()) {
    return false;
  }

  // If filter has group_ids and this maintenance exist in them.
  const auto& groups = filter.get_group_ids();
  if (!groups.empty() &&
      // our group_id is not in the filter list.
      groups.end() ==
          std::find_if(groups.begin(), groups.end(), [&](const auto& group_id) {
            return group_id == def.group_id_ref().value();
          })) {
    return false;
  }
  // none of the filters prevented this maintenance from being
  // filtered out.
  return true;
}

} // namespace APIUtils
}}} // namespace facebook::logdevice::maintenance
