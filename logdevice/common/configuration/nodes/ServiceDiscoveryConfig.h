/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>

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
  using RoleSet = std::bitset<static_cast<size_t>(NodeRole::Count)>;

  /**
   * The IP (v4 or v6) address, including port number.
   */
  Sockaddr address;
  Sockaddr gossip_address;

  /**
   * The IP (v4 or v6) address, including port number, for SSL communication.
   * In production this will mostly be identical to address, except for the
   * port. We need both address and ssl_address, so the server could serve
   * both non-SSL and SSL clients.
   */
  folly::Optional<Sockaddr> ssl_address;

  /**
   * Location information of the node.
   */
  folly::Optional<NodeLocation> location;

  /**
   * Bitmap storing node roles
   */
  RoleSet roles;

  /**
   * A hostname string for the node. Currently we require it to be immutable.
   * Currently logdeviced doesn't use it but some tooling does so we set it in
   * prod. But this can be omitted (as an empty string) in tests.
   */
  std::string hostname;

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
        roles == rhs.roles && hostname == rhs.hostname;
  }

  bool isValid() const;

  // return the corresponding sockaddr for the given socket type
  const Sockaddr& getSockaddr(SocketType type, ConnectionType conntype) const;
};

using ServiceDiscoveryConfig =
    NodeAttributesConfig<NodeServiceDiscovery, /*Mutable=*/false>;

}}}} // namespace facebook::logdevice::configuration::nodes
