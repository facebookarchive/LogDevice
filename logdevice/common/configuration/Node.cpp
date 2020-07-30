/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/Node.h"

#include <folly/dynamic.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice { namespace configuration {

Node::Node(const Node& other) {
  name = other.name;
  address = other.address;
  gossip_address = other.gossip_address;
  ssl_address = other.ssl_address;
  admin_address = other.admin_address;
  server_to_server_address = other.server_to_server_address;
  server_thrift_api_address = other.server_thrift_api_address;
  client_thrift_api_address = other.client_thrift_api_address;
  generation = other.generation;
  location = other.location;
  roles = other.roles;
  tags = other.tags;
  metadata_node = other.metadata_node;

  if (hasRole(NodeRole::SEQUENCER)) {
    sequencer_attributes =
        std::make_unique<SequencerNodeAttributes>(*other.sequencer_attributes);
  }
  if (hasRole(NodeRole::STORAGE)) {
    storage_attributes =
        std::make_unique<StorageNodeAttributes>(*other.storage_attributes);
  }
}

Node& Node::operator=(const Node& node) {
  *this = Node(node);
  return *this;
}

/* static */ Node Node::withTestDefaults(node_index_t idx,
                                         bool sequencer,
                                         bool storage) {
  Node n;

  n.name = folly::sformat("server-{}", idx);

  std::string addr = folly::sformat("127.0.0.{}", idx);
  n.address = Sockaddr(addr, 4440);
  n.gossip_address = Sockaddr(addr, 4441);
  n.admin_address = Sockaddr(addr, 64440);
  n.server_to_server_address = Sockaddr(addr, 4442);
  n.server_thrift_api_address = Sockaddr(addr, 7441);
  n.client_thrift_api_address = Sockaddr(addr, 7440);

  if (sequencer) {
    n.addSequencerRole();
  }
  if (storage) {
    n.addStorageRole();
  }
  return n;
}

std::string Node::locationStr() const {
  if (!location.has_value()) {
    return "";
  }
  return location.value().toString();
}

std::string storageStateToString(StorageState v) {
  switch (v) {
    case StorageState::READ_WRITE:
      return "read-write";
    case StorageState::READ_ONLY:
      return "read-only";
    case StorageState::DISABLED:
      return "disabled";
  }

  // Make the server fail if this func is called with a StorageState
  // that is not included in one of these switch cases.
  ld_check(false);
  return "";
}

bool storageStateFromString(const std::string& str, StorageState* out) {
  ld_check(out);
  if (str == "read-write") {
    *out = StorageState::READ_WRITE;
  } else if (str == "read-only") {
    *out = StorageState::READ_ONLY;
  } else if (str == "disabled" || str == "none") {
    *out = StorageState::DISABLED;
  } else {
    return false;
  }
  return true;
}

std::string toString(NodeRole& v) {
  switch (v) {
    case NodeRole::SEQUENCER:
      return "sequencer";
    case NodeRole::STORAGE:
      return "storage";
  }

  // Make the server fail if this func is called with a NodeRole
  // that is not included in one of these switch cases.
  ld_check(false);
  return "";
}

bool nodeRoleFromString(const std::string& str, NodeRole* out) {
  ld_check(out);
  if (str == "sequencer") {
    *out = NodeRole::SEQUENCER;
  } else if (str == "storage") {
    *out = NodeRole::STORAGE;
  } else {
    return false;
  }
  return true;
}

Node& Node::setTags(std::unordered_map<std::string, std::string> tags) {
  this->tags = std::move(tags);
  return *this;
}
Node& Node::setLocation(const std::string& location) {
  this->location = NodeLocation();
  this->location.value().fromDomainString(location);
  return *this;
}
Node& Node::setIsMetadataNode(bool metadata_node) {
  this->metadata_node = metadata_node;
  return *this;
}
Node& Node::setGeneration(node_gen_t generation) {
  this->generation = generation;
  return *this;
}
Node& Node::setName(std::string name) {
  this->name = std::move(name);
  return *this;
}
Node& Node::setAddress(Sockaddr address) {
  this->address = std::move(address);
  return *this;
}
Node& Node::setGossipAddress(Sockaddr gossip_address) {
  this->gossip_address = std::move(gossip_address);
  return *this;
}
Node& Node::setSSLAddress(Sockaddr ssl_address) {
  this->ssl_address = std::move(ssl_address);
  return *this;
}

}}} // namespace facebook::logdevice::configuration
