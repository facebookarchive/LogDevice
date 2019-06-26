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

bool NodeUpdateBuilder::validate(std::string* reason) const {
  auto set_reason = [reason](std::string r) {
    if (reason != nullptr) {
      *reason = std::move(r);
    }
  };

  if (!node_index_.hasValue()) {
    set_reason("Mandatory field 'node_index' is missing");
    return false;
  }

  if (!data_address_.hasValue()) {
    set_reason("Mandatory field 'data_address' is missing");
    return false;
  }

  if (!name_.hasValue()) {
    set_reason("Mandatory field 'name' is missing");
    return false;
  }

  if (roles_.count() == 0) {
    set_reason("The node is missing the role");
    return false;
  }

  if (hasRole(roles_, NodeRole::SEQUENCER)) {
    if (!sequencer_weight_.hasValue()) {
      set_reason("Sequencer node without field 'sequencer_weight'");
      return false;
    }
  }

  if (hasRole(roles_, NodeRole::STORAGE)) {
    if (!storage_capacity_.hasValue()) {
      set_reason("Storage node without field 'storage_capacity'");
      return false;
    }
    if (!num_shards_.hasValue()) {
      set_reason("Storage node without field 'num_shards'");
      return false;
    }
  }

  return true;
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

bool NodeUpdateBuilder::buildAddNodeUpdate(
    NodesConfiguration::Update& update,
    membership::MembershipVersion::Type sequencer_version,
    membership::MembershipVersion::Type storage_version) && {
  if (!validate()) {
    return false;
  }

  using namespace facebook::logdevice::membership;

  auto node_idx = node_index_.value();

  // Service Discovery Update
  if (update.service_discovery_update == nullptr) {
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
  }
  update.service_discovery_update->addNode(
      node_idx,
      {ServiceDiscoveryConfig::UpdateType::PROVISION,
       buildNodeServiceDiscovery()});

  // Sequencer Config Update
  if (hasRole(roles_, NodeRole::SEQUENCER)) {
    if (update.sequencer_config_update == nullptr) {
      update.sequencer_config_update =
          std::make_unique<SequencerConfig::Update>();
    }
    // Sequencer Membership Update
    if (update.sequencer_config_update->membership_update == nullptr) {
      update.sequencer_config_update->membership_update =
          std::make_unique<SequencerMembership::Update>(sequencer_version);
    }
    update.sequencer_config_update->membership_update->addNode(
        node_idx,
        {SequencerMembershipTransition::ADD_NODE,
         /* enabled= */ false,
         sequencer_weight_.value(),
         MaintenanceID::MAINTENANCE_NONE});

    // Sequencer Config
    if (update.sequencer_config_update->attributes_update == nullptr) {
      update.sequencer_config_update->attributes_update =
          std::make_unique<SequencerAttributeConfig::Update>();
    }
    update.sequencer_config_update->attributes_update->addNode(
        node_idx,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});
  }

  // Storage Config Update
  if (hasRole(roles_, NodeRole::STORAGE)) {
    // Storage Membership Update
    if (update.storage_config_update == nullptr) {
      update.storage_config_update = std::make_unique<StorageConfig::Update>();
    }

    if (update.storage_config_update->membership_update == nullptr) {
      update.storage_config_update->membership_update =
          std::make_unique<StorageMembership::Update>(storage_version);
    }

    for (int shard = 0; shard < num_shards_.value(); shard++) {
      update.storage_config_update->membership_update->addShard(
          ShardID(node_idx, shard),
          {StorageStateTransition::ADD_EMPTY_SHARD,
           Condition::NONE,
           MaintenanceID::MAINTENANCE_NONE});
    }

    // Storage Attributes Update
    if (update.storage_config_update->attributes_update == nullptr) {
      update.storage_config_update->attributes_update =
          std::make_unique<StorageAttributeConfig::Update>();
    }
    update.storage_config_update->attributes_update->addNode(
        node_idx,
        {StorageAttributeConfig::UpdateType::PROVISION,
         buildStorageAttributes()});
  }

  return true;
}

}}}} // namespace facebook::logdevice::configuration::nodes
