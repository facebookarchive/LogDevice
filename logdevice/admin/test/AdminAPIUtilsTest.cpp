/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIUtils.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

namespace {

const std::string kTestAddress = "127.0.0.1";
const std::string kTestNodeName = "test-server";
const std::string kAnotherTestNodeName = "another-test-server";
const std::string kTestUnixPath = "/unix/socket/path";
const node_index_t kTestNodeIndex = 1337;
const node_index_t kAnotherTestNodeIndex = 1007;
const in_port_t kTestDataPort = 4440;
const in_port_t kTestSSLPort = 4443;
const Sockaddr kTestSocketAddress = Sockaddr{kTestAddress, kTestDataPort};
const Sockaddr kAnotherTestSocketAddress = Sockaddr{kTestAddress, kTestSSLPort};

thrift::SocketAddress toThrift(const Sockaddr& address) {
  facebook::logdevice::thrift::SocketAddress result;
  result.set_address(address.getAddress().str());
  result.set_port(address.port());
  return result;
}

} // namespace

TEST(AdminAPIUtilsTest, MatchNodeByName) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.name = kTestNodeName;

  thrift::NodeID thriftNodeId;

  thriftNodeId.set_name(kTestNodeName);
  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));

  thriftNodeId.set_name(kAnotherTestNodeName);
  EXPECT_FALSE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, MatchNodeByIndex) {
  NodeServiceDiscovery nodeServiceDiscovery;

  thrift::NodeID thriftNodeId;
  thriftNodeId.set_node_index(kTestNodeIndex);

  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));

  EXPECT_FALSE(
      nodeMatchesID(kAnotherTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, MatchNodeByAddressIpV4) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = kTestSocketAddress;

  thrift::NodeID thriftNodeId;

  thriftNodeId.set_address(toThrift(kTestSocketAddress));
  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));

  thriftNodeId.set_address(toThrift(kAnotherTestSocketAddress));
  EXPECT_FALSE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, MatchNodeByAddressIpV6WithCompression) {
  std::string compressedV6Address = "2001:4860:4860::8888";
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = Sockaddr{compressedV6Address, kTestDataPort};

  std::string uncompressedV6Address = "2001:4860:4860:0000:0000:0000:0000:8888";
  thrift::NodeID thriftNodeId;
  thriftNodeId.set_address(
      toThrift(Sockaddr{uncompressedV6Address, kTestDataPort}));

  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, MatchNodeByAddressUnixSocket) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.address = Sockaddr{kTestUnixPath};

  thrift::SocketAddress thriftSocketAddress;
  thriftSocketAddress.set_address(kTestUnixPath);
  thriftSocketAddress.set_address_family(thrift::SocketAddressFamily::UNIX);
  thrift::NodeID thriftNodeId;
  thriftNodeId.set_address(thriftSocketAddress);

  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, MatchByNameAndIndex) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.name = kTestNodeName;

  thrift::NodeID thriftNodeId;
  thriftNodeId.set_name(kTestNodeName);
  thriftNodeId.set_node_index(kTestNodeIndex);

  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}

TEST(AdminAPIUtilsTest, EmptyIDMatchesAnything) {
  NodeServiceDiscovery nodeServiceDiscovery;
  nodeServiceDiscovery.name = kTestNodeName;
  nodeServiceDiscovery.address = Sockaddr{kTestAddress, kTestDataPort};

  thrift::NodeID thriftNodeId;

  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));

  // Also matches any unix path
  nodeServiceDiscovery.address = Sockaddr{kTestUnixPath};
  EXPECT_TRUE(
      nodeMatchesID(kTestNodeIndex, nodeServiceDiscovery, thriftNodeId));
}
