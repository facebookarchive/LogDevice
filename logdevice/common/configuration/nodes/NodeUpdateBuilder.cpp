/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/NodeUpdateBuilder.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using namespace facebook::logdevice::membership;

NodeUpdateBuilder& NodeUpdateBuilder::setNodeIndex(node_index_t idx) {
  node_index_ = idx;
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setDataAddress(Sockaddr addr) {
  data_address_ = std::move(addr);
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setGossipAddress(Sockaddr addr) {
  gossip_address_ = std::move(addr);
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setSSLAddress(Sockaddr addr) {
  ssl_address_ = std::move(addr);
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setLocation(NodeLocation loc) {
  location_ = std::move(loc);
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setName(std::string name) {
  name_ = std::move(name);
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::isSequencerNode() {
  roles_[static_cast<size_t>(NodeRole::SEQUENCER)] = true;
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::isStorageNode() {
  roles_[static_cast<size_t>(NodeRole::STORAGE)] = true;
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setNumShards(shard_size_t shards) {
  num_shards_ = shards;
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setStorageCapacity(double cap) {
  storage_capacity_ = cap;
  return *this;
}

NodeUpdateBuilder& NodeUpdateBuilder::setSequencerWeight(double weight) {
  sequencer_weight_ = weight;
  return *this;
}

NodeUpdateBuilder::Result NodeUpdateBuilder::validate() const {
  if (!node_index_.hasValue()) {
    return Result{
        Status::INVALID_PARAM, "Mandatory field 'node_index' is missing"};
  }

  if (!data_address_.hasValue()) {
    return Result{
        Status::INVALID_PARAM, "Mandatory field 'data_address' is missing"};
  }

  if (!name_.hasValue()) {
    return Result{Status::INVALID_PARAM, "Mandatory field 'name' is missing"};
  }

  if (roles_.count() == 0) {
    return Result{Status::INVALID_PARAM, "The node is missing the role"};
  }

  if (hasRole(roles_, NodeRole::SEQUENCER)) {
    if (!sequencer_weight_.hasValue()) {
      return Result{Status::INVALID_PARAM,
                    "Sequencer node without field 'sequencer_weight'"};
    }
  }

  if (hasRole(roles_, NodeRole::STORAGE)) {
    if (!storage_capacity_.hasValue()) {
      return Result{Status::INVALID_PARAM,
                    "Storage node without field 'storage_capacity'"};
    }
    if (!num_shards_.hasValue()) {
      return Result{
          Status::INVALID_PARAM, "Storage node without field 'num_shards'"};
    }
  }

  return {Status::OK, ""};
}

std::unique_ptr<NodeServiceDiscovery>
NodeUpdateBuilder::buildNodeServiceDiscovery() {
  auto sd = std::make_unique<NodeServiceDiscovery>();

  sd->name = std::move(name_).value();
  sd->address = std::move(data_address_).value();
  sd->gossip_address = std::move(gossip_address_);
  sd->ssl_address = std::move(ssl_address_);
  sd->location = std::move(location_);
  sd->roles = roles_;

  return sd;
}

std::unique_ptr<StorageNodeAttribute>
NodeUpdateBuilder::buildStorageAttributes() {
  // TODO(mbassem): Check initial exclude_from_nodesets.
  auto attr = std::make_unique<StorageNodeAttribute>();
  attr->capacity = storage_capacity_.value();
  attr->num_shards = num_shards_.value();
  attr->generation = 1;
  attr->exclude_from_nodesets = false;
  return attr;
}

std::unique_ptr<SequencerNodeAttribute>
NodeUpdateBuilder::buildSequencerAttributes() {
  return std::make_unique<SequencerNodeAttribute>();
}

namespace {
// A helper function to create the Update structure if it's still nullptr
template <class T, typename... Args>
std::unique_ptr<T>& createIfNull(std::unique_ptr<T>& ptr, Args&&... args) {
  if (ptr == nullptr) {
    ptr = std::make_unique<T>(std::forward<Args>(args)...);
  }
  return ptr;
}
} // namespace

NodeUpdateBuilder::Result NodeUpdateBuilder::buildAddNodeUpdate(
    NodesConfiguration::Update& update,
    membership::MembershipVersion::Type sequencer_version,
    membership::MembershipVersion::Type storage_version) && {
  auto validation_result = validate();
  if (validation_result.status != Status::OK) {
    return validation_result;
  }

  auto node_idx = node_index_.value();

  // Service Discovery Update
  createIfNull(update.service_discovery_update)
      ->addNode(node_idx,
                {ServiceDiscoveryConfig::UpdateType::PROVISION,
                 buildNodeServiceDiscovery()});

  // Sequencer Config Update
  if (hasRole(roles_, NodeRole::SEQUENCER)) {
    createIfNull(update.sequencer_config_update);
    // Sequencer Membership Update
    createIfNull(
        update.sequencer_config_update->membership_update, sequencer_version)
        ->addNode(node_idx,
                  {SequencerMembershipTransition::ADD_NODE,
                   /* enabled= */ false,
                   sequencer_weight_.value(),
                   MaintenanceID::MAINTENANCE_NONE});
    // Sequencer Config
    createIfNull(update.sequencer_config_update->attributes_update)
        ->addNode(node_idx,
                  {SequencerAttributeConfig::UpdateType::PROVISION,
                   std::make_unique<SequencerNodeAttribute>()});
  }

  // Storage Config Update
  if (hasRole(roles_, NodeRole::STORAGE)) {
    // Storage Membership Update
    createIfNull(update.storage_config_update);
    createIfNull(
        update.storage_config_update->membership_update, storage_version);

    for (int shard = 0; shard < num_shards_.value(); shard++) {
      update.storage_config_update->membership_update->addShard(
          ShardID(node_idx, shard),
          {StorageStateTransition::ADD_EMPTY_SHARD,
           Condition::NONE,
           MaintenanceID::MAINTENANCE_NONE});
    }

    // Storage Attributes Update
    createIfNull(update.storage_config_update->attributes_update)
        ->addNode(node_idx,
                  {StorageAttributeConfig::UpdateType::PROVISION,
                   buildStorageAttributes()});
  }

  return {Status::OK, ""};
}

NodeUpdateBuilder::Result NodeUpdateBuilder::buildUpdateNodeUpdate(
    NodesConfiguration::Update& update,
    const NodesConfiguration& nodes_configuration) && {
  auto validation_result = validate();
  if (validation_result.status != Status::OK) {
    return validation_result;
  }

  ld_assert(node_index_.hasValue());
  auto node_idx = node_index_.value();

  auto current_svc = nodes_configuration.getNodeServiceDiscovery(node_idx);
  if (current_svc == nullptr) {
    return {E::NOTINCONFIG, folly::sformat("N{} is not in config", node_idx)};
  }

  // Roles should be immutable
  if (roles_ != current_svc->roles) {
    return {E::INVALID_PARAM,
            folly::sformat("The roles field should be immutable. Current "
                           "value: {}, update request: {}",
                           current_svc->roles.to_string(),
                           roles_.to_string())};
  }

  // Compare ServiceDiscovery
  if (auto new_svc = buildNodeServiceDiscovery(); *current_svc != *new_svc) {
    createIfNull(update.service_discovery_update)
        ->addNode(
            node_idx,
            {ServiceDiscoveryConfig::UpdateType::RESET, std::move(new_svc)});
  }

  // Compare Sequencer Attributes
  if (hasRole(roles_, NodeRole::SEQUENCER)) {
    const auto& seq_config = nodes_configuration.getSequencerConfig();
    auto curr_attrs =
        seq_config->getAttributes()->getNodeAttributesPtr(node_idx);
    auto curr_weight = seq_config->getMembership()
                           ->getNodeStatePtr(node_idx)
                           ->getConfiguredWeight();
    auto new_attrs = buildSequencerAttributes();
    if (*curr_attrs != *new_attrs) {
      createIfNull(update.sequencer_config_update);
      createIfNull(update.sequencer_config_update->attributes_update)
          ->addNode(node_idx,
                    {SequencerAttributeConfig::UpdateType::RESET,
                     std::move(new_attrs)});
    }

    if (curr_weight != *sequencer_weight_) {
      createIfNull(update.sequencer_config_update);
      createIfNull(update.sequencer_config_update->membership_update,
                   seq_config->getMembership()->getVersion())
          ->addNode(node_idx,
                    {SequencerMembershipTransition::SET_WEIGHT,
                     /* enabled= */ false /* not used */,
                     sequencer_weight_.value(),
                     MaintenanceID::MAINTENANCE_NONE});
    }
  }

  // Compare storage attributes
  if (hasRole(roles_, NodeRole::STORAGE)) {
    const auto& storage_attrs = nodes_configuration.getStorageAttributes();
    auto curr_attrs = storage_attrs->getNodeAttributesPtr(node_idx);
    auto new_attrs = buildStorageAttributes();
    if (*curr_attrs != *new_attrs) {
      createIfNull(update.storage_config_update);
      createIfNull(update.storage_config_update->attributes_update)
          ->addNode(node_idx,
                    {StorageAttributeConfig::UpdateType::RESET,
                     std::move(new_attrs)});
    }
  }
  return {Status::OK, ""};
}

}}}} // namespace facebook::logdevice::configuration::nodes
