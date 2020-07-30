/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/ServerAddressRouter.h"

#include <gtest/gtest.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"

using namespace facebook::logdevice;

using configuration::nodes::NodeServiceDiscovery;
using configuration::nodes::ServerAddressRouter;
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

class ServerAddressRouterTest : public ::testing::Test {};

// When only the 'address' member is populated, getSockAddr
// should return the default address.
TEST(ServerAddressRouterTest, GetDefaultSockAddr) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// Ditto of above, for clients.
TEST(ServerAddressRouterTest, GetDefaultSockAddrClient) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// 'use_dedicated_server_to_server_address' parameter should be ignored when the
// peer is a client.
TEST(ServerAddressRouterTest, IgnoreServerToServerAddressParam) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// When only the 'address' member is populated, getSockAddr
// should return the default address.
TEST(ServerAddressRouterTest, SslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestSslAddress);
}

// Same as above, for clients.
TEST(ServerAddressRouterTest, SslAddressClient) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ false,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestSslAddress);
}

// When both the 'address' and 'server_to_server_address' members are populated,
// getSockAddr should return the default address if the feature flag is not set.
TEST(ServerAddressRouterTest, DefaultServerToServerIsBaseAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// When both the 'address' and 'server_to_server_address' members are populated,
// getSockAddr should return the server-to-server address when the feature flag
// is set.
TEST(ServerAddressRouterTest, DedicatedServerToServerAddressIfEnabled) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestServerToServerAddress);
}

// When all addresses are populated, it should return the server-to-server
// address if the feature flag is set.
TEST(ServerAddressRouterTest, ServerToServerOverridesSslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.ssl_address = kTestSslAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestServerToServerAddress);
}

// When the gossip address is present and client asks for it, it should be
// returned even if the server-to-server address is set and enabled.
TEST(ServerAddressRouterTest, gossipAddressOverridesData) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.gossip_address = kTestGossipAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::GOSSIP,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, kTestGossipAddress);
}

// The function should return INVALID if
// 'use_dedicated_server_to_server_address' is used but the NodeServiceDiscovery
// object was not constructed with a valid server_to_server_address.
TEST(ServerAddressRouterTest, invalidServerToServerAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, Sockaddr::INVALID);
}

// The function should return INVALID if
// ConnectionType::SSL is used but the NodeServiceDiscovery
// object was not constructed with a valid ssl_address.
TEST(ServerAddressRouterTest, invalidSslAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::DATA,
      ConnectionType::SSL,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ true,
      /* use_dedicated_gossip_port */ true);

  EXPECT_EQ(actual, Sockaddr::INVALID);
}

// If use_dedicated_gossip_port is false, the data port should be used instead.
TEST(ServerAddressRouterTest, dedicatedGossipPortDisabled) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.default_client_data_address = kTestDefaultAddress;
  nodeServiceDiscovery.gossip_address = kTestGossipAddress;

  auto actual = ServerAddressRouter().getAddress(
      0,
      nodeServiceDiscovery,
      SocketType::GOSSIP,
      ConnectionType::PLAIN,
      /* is_server */ true,
      /* use_dedicated_server_to_server_address */ false,
      /* use_dedicated_gossip_port */ false);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

} // namespace
