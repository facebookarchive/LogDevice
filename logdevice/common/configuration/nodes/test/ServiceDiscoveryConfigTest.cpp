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
constexpr in_port_t kTestServerToServerPort = 4442;
const Sockaddr kTestDefaultAddress =
    Sockaddr{kTestAddress.toString(), kTestDataPort};
const Sockaddr kTestServerToServerAddress =
    Sockaddr{kTestAddress.toString(), kTestServerToServerPort};

class ServiceDiscoveryConfigTest : public ::testing::Test {};

TEST(ServiceDiscoveryConfigTest, getSockaddr_GetDefaultSockAddr) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = kTestDefaultAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA, ConnectionType::PLAIN, PeerType::NODE);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

// This test will become the same as
// getSockaddr_DedicatedServerToServerAddressIfEnabled once the 4th argument of
// getSockaddr(...) is removed.
TEST(ServiceDiscoveryConfigTest,
     getSockaddr_DefaultServerToServerIsBaseAddress) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA, ConnectionType::PLAIN, PeerType::NODE);

  EXPECT_EQ(actual, kTestDefaultAddress);
}

TEST(ServiceDiscoveryConfigTest,
     getSockaddr_DedicatedServerToServerAddressIfEnabled) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = kTestDefaultAddress;
  nodeServiceDiscovery.server_to_server_address = kTestServerToServerAddress;

  const Sockaddr& actual = nodeServiceDiscovery.getSockaddr(
      SocketType::DATA, ConnectionType::PLAIN, PeerType::NODE, true);

  EXPECT_EQ(actual, kTestServerToServerAddress);
}

} // namespace
