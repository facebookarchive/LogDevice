/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Optional.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/test/NodeSetTestUtil.h"

using namespace facebook::logdevice;

namespace {
using configuration::nodes::getNodeSSL;

/**
 * An address is needed when creating ServerConfig
 *
 * Testing that if destination does not have SSL settings, should not downgrade
 * Give origin SSL settings just to be sure that it has no impact
 */
#define SSL_DOWNGRADE_TEST_SETUP(origin_str, destination_str)             \
  configuration::Node origin, destination;                                \
  node_index_t id_origin = 1, id_destination = 2;                         \
  NodeLocation location;                                                  \
  NodeLocation location_origin, location_destination;                     \
                                                                          \
  ASSERT_EQ(0, location_origin.fromDomainString(origin_str));             \
  ASSERT_EQ(0, location_destination.fromDomainString(destination_str));   \
                                                                          \
  origin.location = location_origin;                                      \
  destination.location = location_destination;                            \
                                                                          \
  origin.address = Sockaddr("::1", "0");                                  \
  destination.address = Sockaddr("::1", "1");                             \
                                                                          \
  origin.ssl_address = Sockaddr("::1", "2");                              \
  destination.ssl_address = folly::none;                                  \
                                                                          \
  auto nodes =                                                            \
      ServerConfig::Nodes({std::make_pair(id_origin, origin),             \
                           std::make_pair(id_destination, destination)}); \
  for (auto& kv : nodes) {                                                \
    kv.second.addSequencerRole();                                         \
    kv.second.addStorageRole();                                           \
  }                                                                       \
  configuration::NodesConfig nodes_config(std::move(nodes));              \
                                                                          \
  const auto serverConfig = ServerConfig::fromDataTest(                   \
      "server_config_test", std::move(nodes_config));                     \
  const auto nc = serverConfig->getNodesConfigurationFromServerConfigSource();

TEST(NodesConfigurationUtilsTest, NoSslDowngradeSameLocation) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::RACK));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROW));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::CLUSTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::DATA_CENTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

TEST(NodesConfigurationUtilsTest, NoSslDowngradeRack) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::RACK));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROW));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::CLUSTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::DATA_CENTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

TEST(NodesConfigurationUtilsTest, NoSslDowngradeRow) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro1.rk0")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::RACK));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::ROW));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::CLUSTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::DATA_CENTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

TEST(NodesConfigurationUtilsTest, NoSslDowngradeCluster) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl1.ro0.rk0")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::RACK));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::ROW));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::CLUSTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::DATA_CENTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

TEST(NodesConfigurationUtilsTest, NoSslDowngradeDataCenter) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg0.dc1.cl0.ro0.rk0")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::RACK));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::ROW));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::CLUSTER));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::DATA_CENTER));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

TEST(NodesConfigurationUtilsTest, NoSslDowngradeRegion) {
  SSL_DOWNGRADE_TEST_SETUP("rg0.dc0.cl0.ro0.rk0", "rg1.dc0.cl0.ro0.rk0")

  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::NODE));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::RACK));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::ROW));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::CLUSTER));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::DATA_CENTER));
  EXPECT_TRUE(getNodeSSL(*nc,
                         origin.location,
                         NodeID(id_destination).index(),
                         NodeLocationScope::REGION));
  EXPECT_FALSE(getNodeSSL(*nc,
                          origin.location,
                          NodeID(id_destination).index(),
                          NodeLocationScope::ROOT));
}

} // namespace
