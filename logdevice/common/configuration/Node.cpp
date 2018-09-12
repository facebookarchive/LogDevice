/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Node.h"

#include <folly/dynamic.h>
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice { namespace configuration {
constexpr double Node::DEFAULT_STORAGE_CAPACITY;
constexpr StorageState Node::DEFAULT_STORAGE_STATE;

const Sockaddr& Node::getSockaddr(SocketType type) const {
  switch (type) {
    case SocketType::GOSSIP:
      return gossip_address;

    case SocketType::SSL:
      if (!ssl_address.hasValue()) {
        return Sockaddr::INVALID;
      }
      return ssl_address.value();

    case SocketType::DATA:
      return address;

    default:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1), 2, "Unexpected Socket Type:%d!", (int)type);
      ld_check(false);
  }

  return Sockaddr::INVALID;
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
    case StorageState::NONE:
      return "none";
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
  } else if (str == "none") {
    *out = StorageState::NONE;
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
