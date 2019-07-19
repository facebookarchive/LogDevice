/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/NodesConfigParser.h"

#include <folly/dynamic.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/NodesConfig.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

//
// Forward declarations of helpers
//

static bool parseOneNode(const folly::dynamic&,
                         node_index_t&,
                         Configuration::Node&);

// Common to all node roles
static bool parseNodeID(const folly::dynamic&, node_index_t&);
static bool parseGeneration(const folly::dynamic&, Configuration::Node&);
static bool parseHostFields(const folly::dynamic&,
                            std::string& /* name */,
                            Sockaddr&, /* host address */
                            Sockaddr&, /* gossip address */
                            folly::Optional<Sockaddr>& /* ssl address */);
static bool parseLocation(const folly::dynamic&, Configuration::Node&);
static bool parseRoles(const folly::dynamic&, Configuration::Node&);
using RoleParser = bool(const folly::dynamic&, Configuration::Node&);

// Sequencer role attributes
static RoleParser parseSequencerAttributes;
static bool parseSequencer(const folly::dynamic&, Configuration::Node&);

// Storage role attributes
static RoleParser parseStorageAttributes;
static bool parseNumShards(const folly::dynamic&, Configuration::Node&);
static bool parseStorageState(const folly::dynamic&, Configuration::Node&);

static bool parseRole(NodeRole role,
                      const folly::dynamic& nodeMap,
                      Configuration::Node& output) {
  switch (role) {
    case NodeRole::SEQUENCER:
      output.setRole(role);
      return parseSequencerAttributes(nodeMap, output);
    case NodeRole::STORAGE:
      output.setRole(role);
      return parseStorageAttributes(nodeMap, output);
    default:
      return false;
  }
}

bool parseNodes(const folly::dynamic& clusterMap,
                ServerConfig::NodesConfig& output) {
  auto iter = clusterMap.find("nodes");
  if (iter == clusterMap.items().end()) {
    ld_error("missing \"nodes\" entry for cluster");
    err = E::INVALID_CONFIG;
    return false;
  }

  const folly::dynamic& nodesList = iter->second;
  if (!nodesList.isArray()) {
    ld_error("\"nodes\" entry for cluster must be array");
    err = E::INVALID_CONFIG;
    return false;
  }

  size_t ssl_enabled_nodes = 0;

  std::unordered_map<std::string, node_index_t> seen_names;
  std::unordered_map<Sockaddr, node_index_t, Sockaddr::Hash> seen_addresses;

  ld_check(output.getNodes().empty());
  Nodes res;
  for (const folly::dynamic& item : nodesList) {
    node_index_t index;
    Configuration::Node node;
    if (!parseOneNode(item, index, node)) {
      // NOTE: one invalid node fails the entire config
      return false;
    }

    if (res.count(index)) {
      ld_error("Duplicate node_id in nodes config: %d.", index);
      err = E::INVALID_CONFIG;
      return false;
    }

    if (node.ssl_address) {
      ++ssl_enabled_nodes;
    }

    {
      // Validate the uniqness of the address
      auto insert_result = seen_addresses.emplace(node.address, index);
      if (!insert_result.second) {
        ld_error("Multiple nodes with the same 'host' entry %s "
                 "(indexes %hd and %hd)",
                 node.address.toString().c_str(),
                 insert_result.first->second,
                 index);
        err = E::INVALID_CONFIG;
        return false;
      }
    }

    {
      // Validate the uniqness of the name if it exists
      if (!node.name.empty()) {
        auto insert_result =
            seen_names.emplace(nodes::normalizeServerName(node.name), index);
        if (!insert_result.second) {
          ld_error("Multiple nodes with the same 'name' entry '%s' "
                   "(indexes %hd and %hd)",
                   node.name.c_str(),
                   insert_result.first->second,
                   index);
          err = E::INVALID_CONFIG;
          return false;
        }
      }
    }

    res[index] = std::move(node);
  }

  if (ssl_enabled_nodes != 0 && ssl_enabled_nodes != res.size()) {
    ld_error("SSL has to be enabled for all nodes if it is enabled for at "
             "least one node");
    err = E::INVALID_CONFIG;
    return false;
  }

  // TODO(T15517759): At this point we expect all production tiers to have the
  // same number of shards on each storage node... because rebuilding has not
  // been modified to support heterogeneous hardware. Until this gets
  // implemented, we validate here that all storage nodes have the same number
  // of shards.
  folly::Optional<uint8_t> num_shards;
  for (const auto& it : res) {
    if (!it.second.isReadableStorageNode()) {
      continue;
    }
    if (!num_shards.hasValue()) {
      num_shards = it.second.getNumShards();
    } else if (num_shards.value() != it.second.getNumShards()) {
      ld_error("Not all nodes are configured with the same value for "
               "num_shards. LogDevice does not yet support running tiers "
               "with heterogeneous number of shards.");
      err = E::INVALID_CONFIG;
      return false;
    }
  }

  output.setNodes(std::move(res));

  return true;
}

static bool parseOneNode(const folly::dynamic& nodeMap,
                         node_index_t& out_node_index,
                         Configuration::Node& output) {
  if (!nodeMap.isObject()) {
    ld_error("node config must be map");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (!parseNodeID(nodeMap, out_node_index)) {
    ld_error("\"node_id\" must be present in a node config");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (out_node_index < 0) {
    ld_error("\"node_id\" must be nonnegative, %d found", out_node_index);
    err = E::INVALID_CONFIG;
    return false;
  }

  // Every node entry must have a generation number
  if (!parseGeneration(nodeMap, output)) {
    ld_error("\"generation\" must be present in a node config");
    err = E::INVALID_CONFIG;
    return false;
  }

  return parseHostFields(nodeMap,
                         output.name,
                         output.address,
                         output.gossip_address,
                         output.ssl_address) &&
      parseLocation(nodeMap, output) && parseRoles(nodeMap, output);
}

static bool parseStorageAttributes(const folly::dynamic& nodeMap,
                                   Configuration::Node& output) {
  ld_check(output.hasRole(NodeRole::STORAGE));
  output.addStorageRole();
  return parseNumShards(nodeMap, output) && parseStorageState(nodeMap, output);
}

static bool parseNodeID(const folly::dynamic& nodeMap,
                        node_index_t& out_node_index) {
  return getIntFromMap<node_index_t>(nodeMap, "node_id", out_node_index);
}

static bool parseGeneration(const folly::dynamic& nodeMap,
                            Configuration::Node& output) {
  return getIntFromMap<int>(nodeMap, "generation", output.generation);
}

static bool parseNumShards(const folly::dynamic& nodeMap,
                           Configuration::Node& output) {
  auto* storage = output.storage_attributes.get();
  bool ret =
      getIntFromMap<shard_size_t>(nodeMap, "num_shards", storage->num_shards);
  if (!ret) {
    // "num_shards" property not found.
    ld_error("\"num_shards\" must be present in a node config for all "
             "storage nodes");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (storage->num_shards > MAX_SHARDS) {
    ld_error("\"num_shards\" must be <= %lu", MAX_SHARDS);
    err = E::INVALID_CONFIG;
    return false;
  }

  if (storage->num_shards < 0) {
    ld_error("\"num_shards\" must be >= 0");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (storage->num_shards == 0) {
    ld_error("\"num_shards\" must be > 0 for storage nodes");
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

bool parseHostString(const std::string& hostStr,
                     Sockaddr& addr_out,
                     const std::string& fieldName) {
  auto result = Sockaddr::fromString(hostStr);
  if (!result.hasValue()) {
    ld_error(
        "invalid \"%s\" entry: \"%s\"", fieldName.c_str(), hostStr.c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  addr_out = result.value();
  return true;
}

static bool parseHostFields(const folly::dynamic& nodeMap,
                            std::string& name,
                            Sockaddr& addr_out,
                            Sockaddr& gossip_addr_out,
                            folly::Optional<Sockaddr>& ssl_addr_out) {
  std::string hostStr;
  std::string gossipAddressStr;
  std::string sslHostStr;
  int sslPort, gossipPort;

  // Parse the name of the node, if it's there
  // TODO(T44427489): Make the name field a required field.
  if (getStringFromMap(nodeMap, "name", name)) {
    std::string failure_reason;
    if (!nodes::isValidServerName(name, &failure_reason)) {
      ld_error(
          "Invalid name field %s: %s", name.c_str(), failure_reason.c_str());
    }
  } else {
    name = "";
  }

  if (!getStringFromMap(nodeMap, "host", hostStr)) {
    ld_error("missing \"host\" entry for node");
    err = E::INVALID_CONFIG;
    return false;
  }
  if (!parseHostString(hostStr, addr_out, "host")) {
    ld_warning("parseHostString() failed for host:%s", hostStr.c_str());
    // err set by parseHostString()
    return false;
  }
  ld_check(hostStr.length() > 0);

  if (getIntFromMap<int>(nodeMap, "gossip_port", gossipPort)) {
    size_t posn = hostStr.find_last_of(":");
    std::string host_prefix = hostStr.substr(0, posn + 1);
    gossipAddressStr = host_prefix;
    gossipAddressStr += folly::to<std::string>(gossipPort);
  } else {
    // Port not found, looking for unix domain socket address
    if (!getStringFromMap(nodeMap, "gossip_address", gossipAddressStr)) {
      // Fall back to data port
      gossipAddressStr = hostStr;
    }
  }

  if (!parseHostString(gossipAddressStr, gossip_addr_out, "gossip_address")) {
    ld_warning("parseHostString() failed for gossip_address:%s",
               gossipAddressStr.c_str());
    // err set by parseHostString()
    return false;
  }

  // TODO: consider allowing SSL-only node configuration. For internal
  // communication we could potentially use SSL with no cipher, if openssl
  // supports that?

  // We expect primarily the "ssl_port" parameter to be used in production.
  // However, for testing (where unix sockets are used), we need an "ssl_host"
  // param as well, thus both params are supported.
  if (!getIntFromMap<int>(nodeMap, "ssl_port", sslPort)) {
    sslPort = 0;
  }
  if (getStringFromMap(nodeMap, "ssl_host", sslHostStr)) {
    if (sslPort) {
      ld_error("invalid \"ssl_host\" entry for node: \"%s\", contains both "
               "ssl_port and ssl_host",
               hostStr.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
    Sockaddr sslAddress;
    if (!parseHostString(sslHostStr, sslAddress, "ssl_host")) {
      // err set by parseHostString()
      return false;
    }
    ssl_addr_out.assign(sslAddress);
  } else if (sslPort) {
    if (addr_out.isUnixAddress()) {
      ld_error("invalid \"ssl_port\" entry for node: \"%s\", unix socket "
               "specified as \"host\"",
               hostStr.c_str());
      return false;
    }
    ssl_addr_out.assign(addr_out.withPort(sslPort));
  }

  return true;
}
static bool parseLocation(const folly::dynamic& nodeMap,
                          Configuration::Node& output) {
  std::string location_str;
  bool success = getStringFromMap(nodeMap, "location", location_str);
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"location\" attribute of "
             "node '%s'. Expected a string.",
             output.address.toString().c_str());
    err = E::INVALID_CONFIG;
    return false;
  }

  if (success) {
    /**
     * T7915297 Temporary backward compatibility hack, will remove once
     *          legacy deployments are upgraded
     */
    std::vector<std::string> tokens;
    folly::split(
        NodeLocation::DELIMITER, location_str, tokens, /* ignoreEmpty */ false);
    if (tokens.size() == NodeLocation::NUM_SCOPES - 1) {
      // possible old format, manually create the REGION label by
      // right-stripping digits from DATA_CENTER label
      std::string region_label(tokens[0]);
      region_label.erase(std::find_if(region_label.rbegin(),
                                      region_label.rend(),
                                      [](char c) { return !std::isdigit(c); })
                             .base(),
                         region_label.end());
      location_str = region_label + NodeLocation::DELIMITER + location_str;
    }
    /* end T7915297 */

    NodeLocation location;
    int rv = location.fromDomainString(location_str);
    if (rv != 0) {
      ld_error("Invalid \"location\" string '%s' for node '%s'",
               location_str.c_str(),
               output.address.toString().c_str());
      err = E::INVALID_CONFIG;
      return false;
    }

    ld_check(!location.isEmpty());
    output.location.assign(std::move(location));
  }

  return true;
}

static bool parseSequencerAttributes(const folly::dynamic& nodeMap,
                                     Configuration::Node& output) {
  ld_check(output.hasRole(NodeRole::SEQUENCER));
  output.addSequencerRole();
  return parseSequencer(nodeMap, output);
}

static bool parseSequencer(const folly::dynamic& nodeMap,
                           Configuration::Node& output) {
  // Sequencing is controlled by two attributes:
  // 'sequencer':        This is a bool that enables or disables sequencing.
  //                     The node must also be provisioned with the 'sequencer'
  //                     role. If omitted, sequencing is disabled.
  // 'sequencer_weight': This is a double that controls the amount of logs
  //                     this node should sequence relative to other nodes
  //                     in the cluster. If omitted, weight defaults to 1.

  auto* sequencer = output.sequencer_attributes.get();

  {
    // Parse the old sequencer format if the sequencer field is a double
    double weight = 0;
    if (getDoubleFromMap(nodeMap, "sequencer", weight)) {
      if (weight < 0) {
        ld_error("Sequencer attribute is expected to be a non-negative "
                 "floating point number.");
        err = E::INVALID_CONFIG;
        return false;
      }
      sequencer->setEnabled(true);
      sequencer->setWeight(weight);
      return true;
    }
  }

  // Parse sequencer field
  bool sequencing_enabled = false;
  if (!getBoolFromMap(nodeMap, "sequencer", sequencing_enabled) &&
      err != E::NOTFOUND) {
    // "sequencer" exists, but is not a bool.
    ld_error("Invalid value of 'sequencer' attribute. Expected a bool.");
    err = E::INVALID_CONFIG;
    return false;
  }

  // Parse sequencer_weight field
  err = E::OK;
  double sequencer_weight = sequencing_enabled ? 1.0 : 0.0;
  if (!getDoubleFromMap(nodeMap, "sequencer_weight", sequencer_weight) &&
      err != E::NOTFOUND) {
    ld_error("Invalid type for 'sequencer_weight' attribute. Expected a "
             "double");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (sequencer_weight < 0) {
    ld_error("Invalid value of 'sequencer_weight' attribute. Expected a "
             "non-negative, floating point number");
    err = E::INVALID_CONFIG;
    return false;
  }

  if (sequencing_enabled && sequencer_weight == 0) {
    ld_warning("Enabled sequencer has a weight of 0. This sequencer will "
               "not orchestrate the writes for any logs. Prefer setting "
               "'sequencer' to false when disabling sequencing on a node.");
  }

  sequencer->setEnabled(sequencing_enabled);
  sequencer->setWeight(sequencer_weight);
  return true;
}

static bool parseRoles(const folly::dynamic& nodeMap,
                       Configuration::Node& output) {
  auto iter = nodeMap.find("roles");
  if (iter == nodeMap.items().end()) {
    /* The role attribute is required. It may be empty, but not missing. */
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "no \"roles\" entry in config. Failing config load.");
    return false;
  }

  const folly::dynamic& rolesList = iter->second;
  if (!rolesList.isArray()) {
    /* The roles entry is not an array */
    ld_error("\"roles\" entry for nodes must be an array");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto get_node_index = [](const folly::dynamic& node_map) {
    node_index_t out_node_index = NODE_INDEX_INVALID;
    // Already parsed earlier. Should succeed here.
    bool have_node_index = parseNodeID(node_map, out_node_index);
    ld_check(have_node_index);
    return out_node_index;
  };

  /* Parse roles in config */
  if (rolesList.empty()) {
    ld_info("No roles configured for N%d", get_node_index(nodeMap));
    return true;
  }

  for (auto& role : rolesList) {
    if (!role.isString()) {
      ld_error("found invalid entry on \"roles\" array. expected one of "
               "\"sequencer\" or \"storage\"");
      err = E::INVALID_CONFIG;
      return false;
    }

    NodeRole nodeRole;
    std::string roleStr = role.asString();
    if (!nodeRoleFromString(roleStr, &nodeRole) ||
        !parseRole(nodeRole, nodeMap, output)) {
      ld_error("found unknown \"roles\" entry: %s", roleStr.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
  }

  if (!output.hasRole(configuration::NodeRole::STORAGE) &&
      output.generation != 1) {
    ld_error("Config validate failed: Node %d does not have storage role set "
             "but has a generation %d > 1.",
             get_node_index(nodeMap),
             output.generation);
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

static bool parseStorageState(const folly::dynamic& nodeMap,
                              Configuration::Node& output) {
  folly::Optional<StorageState> legacy_storage_state;
  folly::Optional<double> legacy_storage_capacity;
  int legacy_weight = 1;

  /* Legacy weight parsing */
  if (getIntFromMap<int>(nodeMap, "weight", legacy_weight)) {
    if (!output.hasRole(NodeRole::STORAGE)) {
      ld_error("Invalid 'weight' attribute. 'weight' can only be set "
               "on storage nodes");
      err = E::INVALID_CONFIG;
      return false;
    }

    if (legacy_weight < -1) {
      ld_error("found invalid \"weight\" entry: %d", legacy_weight);
      err = E::INVALID_CONFIG;
      return false;
    }
    if (legacy_weight == -1) {
      legacy_storage_state.assign(StorageState::DISABLED);
    } else if (legacy_weight == 0) {
      legacy_storage_state.assign(StorageState::READ_ONLY);
    } else {
      ld_check(legacy_weight > 0);
      legacy_storage_state.assign(StorageState::READ_WRITE);
      legacy_storage_capacity.assign(legacy_weight);
    }
  } else if (err != E::NOTFOUND && output.hasRole(NodeRole::STORAGE)) {
    ld_error("Invalid \"weight\" entry for node. Expected an "
             "integer >= -1");
    return false;
  }

  ld_check(legacy_weight >= -1);

  /* New storage_state + storage_capacity parsing */
  folly::Optional<StorageState> storage_state;
  folly::Optional<double> storage_capacity;

  std::string storage_str;
  if (getStringFromMap(nodeMap, "storage", storage_str)) {
    if (!output.hasRole(NodeRole::STORAGE)) {
      ld_error("Invalid 'storage' attribute. 'storage' can only be set "
               "on storage nodes");
      err = E::INVALID_CONFIG;
      return false;
    }

    StorageState tmp;
    if (storageStateFromString(storage_str, &tmp)) {
      storage_state.assign(tmp);
    } else {
      ld_error("Invalid value for \"storage\" entry: \"%s\". Expected one of "
               "\"read-write\", \"read-only\" or \"none\"",
               storage_str.c_str());
      err = E::INVALID_CONFIG;
      return false;
    }
  } else if (err != E::NOTFOUND && output.hasRole(NodeRole::STORAGE)) {
    ld_error("Invalid value for \"storage\" entry. Expected one of "
             "\"read-write\", \"read-only\" or \"none\"");
    return false;
  }

  double tmp;
  if (getDoubleFromMap(nodeMap, "storage_capacity", tmp)) {
    if (!output.hasRole(NodeRole::STORAGE)) {
      ld_error("Invalid 'storage_capacity' attribute. 'storage_capacity' can "
               "only be set on storage nodes");
      err = E::INVALID_CONFIG;
      return false;
    }

    storage_capacity.assign(tmp);
  } else if (err != E::NOTFOUND && output.hasRole(NodeRole::STORAGE)) {
    ld_error("Invalid \"storage_capacity\" entry for node. Expected a "
             "non-negative integer.");
    return false;
  }

  // Decide on final values based on presence of either old or new attributes
  // and defaults
  if (!storage_state.hasValue() && !legacy_storage_state.hasValue()) {
    // Use default settings as set by StorageNodeAttributes constructor.
    storage_state.assign(output.storage_attributes->state);
  } else if (storage_state.hasValue() && legacy_storage_state.hasValue()) {
    // both settings specified, they should be identical
    if (storage_state.value() != legacy_storage_state.value()) {
      ld_error("Conflicting properties: \"storage\": \"%s\", weight: %d",
               storage_str.c_str(),
               legacy_weight);
      err = E::INVALID_CONFIG;
      return false;
    }
  } else if (legacy_storage_state.hasValue()) {
    // Legacy value only, use it
    storage_state = legacy_storage_state;
  }
  ld_check(storage_state.hasValue());

  if (storage_capacity.hasValue() && legacy_storage_capacity.hasValue()) {
    // both settings specified, they should be identical. However, weight was
    // an int and storage_capacity is a float, so a rounding discrepancy is
    // expected (and values in the range (0,1) all get rounded up to 1)
    double weight_compatible_storage_capacity = storage_capacity.value() == 0.0
        ? 0.0
        : std::max(1.0, std::round(storage_capacity.value()));
    if (weight_compatible_storage_capacity != legacy_storage_capacity.value()) {
      ld_error("Conflicting properties: \"storage_capacity\": %f (rounded to "
               "%f), weight: %d",
               storage_capacity.value(),
               weight_compatible_storage_capacity,
               legacy_weight);
      err = E::INVALID_CONFIG;
      return false;
    }
  } else if (legacy_storage_capacity.hasValue()) {
    // Legacy value only, use it
    storage_capacity = legacy_storage_capacity;
  }

  // Making sure that we can't have a storage node enabled for writes with 0
  // capacity
  if (storage_state.value() == StorageState::READ_WRITE &&
      storage_capacity.value_or(1) == 0) {
    ld_error(
        "Storage capacity cannot be zero for a node in \"read-write\" state");
    err = E::INVALID_CONFIG;
    return false;
  }

  auto* storage = output.storage_attributes.get();
  storage->state = storage_state.value();
  storage->capacity = storage_capacity.value_or(1);

  if (!getBoolFromMap(
          nodeMap, "exclude_from_nodesets", storage->exclude_from_nodesets)) {
    if (err != E::NOTFOUND) {
      ld_error(
          "Invalid value for \"exclude_from_nodesets\". Expecting true/false");
      err = E::INVALID_CONFIG;
      return false;
    }
    storage->exclude_from_nodesets = false;
  }
  return true;
}

}}}} // namespace facebook::logdevice::configuration::parser
