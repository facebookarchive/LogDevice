/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"

#include <gtest/gtest.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"

using namespace facebook::logdevice;

using configuration::nodes::NodeServiceDiscovery;
using folly::StringPiece;

namespace {

constexpr StringPiece kTestAddress = "127.0.0.1";
constexpr in_port_t kTestDataPort = 4440;
constexpr in_port_t kTestGossipPort = 4441;
constexpr in_port_t kTestServerToServerPort = 4442;
constexpr in_port_t kTestDataSslPort = 4444;
const Sockaddr kTestDefaultAddress =
    Sockaddr{kTestAddress.toString(), kTestDataPort};
const Sockaddr kTestSslAddress =
    Sockaddr{kTestAddress.toString(), kTestDataSslPort};
const Sockaddr kTestGossipAddress =
    Sockaddr{kTestAddress.toString(), kTestGossipPort};
const Sockaddr kTestServerToServerAddress =
    Sockaddr{kTestAddress.toString(), kTestServerToServerPort};

class ServiceDiscoveryConfigTest : public ::testing::Test {};

// When only the 'address' member is populated, getSockAddr
// should return the default address.
TEST(ServiceDiscoveryConfigTest, getSockaddr_GetDefaultSockAddr) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// Ditto of above, for clients.
TEST(ServiceDiscoveryConfigTest, getSockaddr_GetDefaultSockAddrClient) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ false);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// 'use_dedicated_server_to_server_address' parameter should be ignored when the
// peer is a client.
TEST(ServiceDiscoveryConfigTest, getSockaddr_IgnoreServerToServerAddressParam) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// When only the 'address' member is populated, getSockAddr
// should return the default address.
TEST(ServiceDiscoveryConfigTest, getSockaddr_SslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false);

  EXPECT_EQ(actual, kTestSslAddress);
}

// Same as above, for clients.
TEST(ServiceDiscoveryConfigTest, getSockaddr_SslAddressClient) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ false);

  EXPECT_EQ(actual, kTestSslAddress);
}

// When both the 'address' and 'server_to_server_address' members are populated,
// getSockAddr should return the default address if the feature flag is not set.
TEST(ServiceDiscoveryConfigTest,
     getSockaddr_DefaultServerToServerIsBaseAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// When both the 'address' and 'server_to_server_address' members are populated,
// getSockAddr should return the server-to-server address when the feature flag
// is set.
TEST(ServiceDiscoveryConfigTest,
     getSockaddr_DedicatedServerToServerAddressIfEnabled) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, kTestServerToServerAddress);
}

// When all addresses are populated, it should return the server-to-server
// address if the feature flag is set.
TEST(ServiceDiscoveryConfigTest,
     getSockaddr_ServerToServerOverridesSslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, kTestServerToServerAddress);
}

// When the gossip address is present and client asks for it, it should be
// returned even if the server-to-server address is set and enabled.
TEST(ServiceDiscoveryConfigTest, getSockaddr_gossipAddressOverridesData) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.gossip_address = kTestGossipAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::GOSSIP,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, kTestGossipAddress);
}

// The function should return INVALID if
// 'use_dedicated_server_to_server_address' is used but the NodeServiceDiscovery
// object was not constructed with a valid server_to_server_address.
TEST(ServiceDiscoveryConfigTest, getSockaddr_invalidServerToServerAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, Sockaddr::INVALID);
}

// The function should return INVALID if
// ConnectionType::SSL is used but the NodeServiceDiscovery
// object was not constructed with a valid ssl_address.
TEST(ServiceDiscoveryConfigTest, getSockaddr_invalidSslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true);

  EXPECT_EQ(actual, Sockaddr::INVALID);
}

} // namespace
