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
  generation = other.generation;
  location = other.location;
  roles = other.roles;
  if (hasRole(NodeRole::SEQUENCER)) {
    sequencer_attributes =
        std::make_unique<SequencerNodeAttributes>(*other.sequencer_attributes);
  }
  if (hasRole(NodeRole::STORAGE)) {
    storage_attributes =
        std::make_unique<StorageNodeAttributes>(*other.storage_attributes);
  }
}

std::string Node::locationStr() const {
  if (!location.hasValue()) {
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

}}} // namespace facebook::logdevice::configuration
