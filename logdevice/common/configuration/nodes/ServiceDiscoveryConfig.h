/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/nodes/NodeAttributesConfig.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

struct NodeServiceDiscovery {
  using RoleSet = configuration::nodes::RoleSet;

  /*
   * This is a unique name for the node in the cluster. This is currently not a
   * required field and can be empty.
   */
  std::string name{};

  /**
   * The IP (v4 or v6) address, including port number.
   */
  Sockaddr address{};

  /**
   * The IP (v4 or v6) gossip address, including port number. Semantically, if
   * it's folly::none, it means that the gossip connections should go to the
   * data address.
   */
  folly::Optional<Sockaddr> gossip_address{};

  /**
   * The IP (v4 or v6) address, including port number, for SSL communication.
   * In production this will mostly be identical to address, except for the
   * port. We need both address and ssl_address, so the server could serve
   * both non-SSL and SSL clients.
   */
  folly::Optional<Sockaddr> ssl_address{};

  /**
   * Location information of the node.
   */
  folly::Optional<NodeLocation> location{};

  /**
   * Bitmap storing node roles
   */
  RoleSet roles{};

  const Sockaddr& getGossipAddress() const;

  bool hasRole(NodeRole role) const {
    auto id = static_cast<size_t>(role);
    return roles.test(id);
  }

  std::string locationStr() const {
    if (!location.hasValue()) {
      return "";
    }
    return location.value().toString();
  }

  bool operator==(const NodeServiceDiscovery& rhs) const {
    return address == rhs.address && gossip_address == rhs.gossip_address &&
        ssl_address == rhs.ssl_address && location == rhs.location &&
        roles == rhs.roles && name == rhs.name;
  }

  bool operator!=(const NodeServiceDiscovery& rhs) const {
    return !(*this == rhs);
  }

  bool isValid() const;
  std::string toString() const;
  bool isValidForReset(const NodeServiceDiscovery& current) const;

  // return the corresponding sockaddr for the given socket type
  const Sockaddr& getSockaddr(SocketType type, ConnectionType conntype) const;

  const RoleSet& getRoles() const {
    return roles;
  }
};

using ServiceDiscoveryConfig = NodeAttributesConfig<NodeServiceDiscovery>;

}}}} // namespace facebook::logdevice::configuration::nodes
