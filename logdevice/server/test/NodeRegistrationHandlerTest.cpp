/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/NodeRegistrationHandler.h"

#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;

class NodeRegistrationHandlerTest : public ::testing::Test {
 public:
  NodeRegistrationHandlerTest() {
    store_ = std::make_shared<InMemNodesConfigurationStore>(
        "/foo", NodesConfigurationCodec::extractConfigVersion);

    // Write an empty NC
    auto ser_nc = NodesConfigurationCodec::serialize(NodesConfiguration{});
    store_->updateConfigSync(ser_nc, folly::none);
  }

  ServerSettings buildServerSettings(std::string name) {
    ServerSettings settings = create_default_settings<ServerSettings>();

    settings.name = name;
    settings.unix_socket = folly::format("/{}/address", name).str();
    settings.ssl_unix_socket = folly::format("/{}/ssl", name).str();
    settings.gossip_unix_socket = folly::format("/{}/gossip", name).str();
    settings.command_unix_socket = folly::format("/{}/command", name).str();
    settings.roles = 3 /* sequencer + storage */;
    settings.sequencer_weight = 12;
    settings.storage_capacity = 13;
    settings.num_shards = 3;
    settings.admin_enabled = true;

    NodeLocation loc;
    loc.fromDomainString("aa.bb.cc.dd.ee");
    settings.location = loc;

    return settings;
  }

  AdminServerSettings buildAdminServerSettings(std::string name) {
    AdminServerSettings settings =
        create_default_settings<AdminServerSettings>();
    settings.admin_unix_socket = folly::format("/{}/admin", name).str();
    return settings;
  }

  folly::Expected<node_index_t, E> registerNode(ServerSettings set,
                                                AdminServerSettings admin_set) {
    NodeRegistrationHandler handler{
        std::move(set), std::move(admin_set), getNodesConfiguration(), store_};
    return handler.registerSelf(NodeIndicesAllocator{});
  }

  std::shared_ptr<const NodesConfiguration> getNodesConfiguration() {
    std::string ser;
    ld_check_eq(Status::OK, store_->getConfigSync(&ser));
    return NodesConfigurationCodec::deserialize(ser);
  }

  std::shared_ptr<configuration::nodes::NodesConfigurationStore> store_;
};

TEST_F(NodeRegistrationHandlerTest, testAddNode) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");
  auto res = registerNode(settings, admin_settings);
  ASSERT_TRUE(res.hasValue());

  // Get NC
  auto nc = getNodesConfiguration();
  auto index = res.value();

  EXPECT_EQ(1, nc->getServiceDiscovery()->numNodes());
  const auto& svd = nc->getNodeServiceDiscovery(index);
  ASSERT_NE(nullptr, svd);
  EXPECT_EQ("node1", svd->name);
  EXPECT_EQ("/node1/address", svd->address.toString());
  EXPECT_EQ("/node1/ssl", svd->ssl_address->toString());
  EXPECT_EQ("/node1/gossip", svd->gossip_address->toString());
  EXPECT_EQ("/node1/admin", svd->admin_address->toString());
  EXPECT_EQ(RoleSet(3), svd->roles);
  EXPECT_EQ("aa.bb.cc.dd.ee", svd->location->toString());

  const auto& storage_attr = nc->getNodeStorageAttribute(index);
  ASSERT_NE(nullptr, storage_attr);
  EXPECT_EQ(1, storage_attr->generation);
  EXPECT_EQ(13, storage_attr->capacity);
  EXPECT_EQ(3, storage_attr->num_shards);

  const auto& shards = nc->getStorageMembership()->getShardStates(index);
  ASSERT_EQ(3, shards.size());
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(0).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(1).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(2).storage_state);

  const auto& seq = nc->getSequencerMembership()->getNodeState(index);
  ASSERT_TRUE(seq.hasValue());
  EXPECT_FALSE(seq->sequencer_enabled);
  EXPECT_EQ(12, seq->getConfiguredWeight());
}

TEST_F(NodeRegistrationHandlerTest, testUpdate) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");
  auto add_res = registerNode(settings, admin_settings);
  ASSERT_TRUE(add_res.hasValue());
  auto index = add_res.value();

  settings.unix_socket = "/new/address";
  settings.ssl_unix_socket = "/new/ssl";
  settings.gossip_unix_socket = "/new/gossip";
  settings.sequencer_weight = 22;
  settings.storage_capacity = 23;
  settings.admin_enabled = false;

  NodeRegistrationHandler handler{
      settings, admin_settings, getNodesConfiguration(), store_};
  auto status = handler.updateSelf(index);
  ASSERT_EQ(Status::OK, status);

  // Get NC
  auto nc = getNodesConfiguration();
  const auto& svd = nc->getNodeServiceDiscovery(index);
  ASSERT_NE(nullptr, svd);
  EXPECT_EQ("node1", svd->name);
  EXPECT_EQ("/new/address", svd->address.toString());
  EXPECT_EQ("/new/ssl", svd->ssl_address->toString());
  EXPECT_EQ("/new/gossip", svd->gossip_address->toString());
  EXPECT_FALSE(svd->admin_address.hasValue());
  EXPECT_EQ(RoleSet(3), svd->roles);
  EXPECT_EQ("aa.bb.cc.dd.ee", svd->location->toString());

  const auto& storage_attr = nc->getNodeStorageAttribute(index);
  ASSERT_NE(nullptr, storage_attr);
  EXPECT_EQ(1, storage_attr->generation);
  EXPECT_EQ(23, storage_attr->capacity);
  EXPECT_EQ(3, storage_attr->num_shards);

  const auto& shards = nc->getStorageMembership()->getShardStates(index);
  ASSERT_EQ(3, shards.size());
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(0).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(1).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(2).storage_state);

  const auto& seq = nc->getSequencerMembership()->getNodeState(index);
  ASSERT_TRUE(seq.hasValue());
  EXPECT_FALSE(seq->sequencer_enabled);
  EXPECT_EQ(22, seq->getConfiguredWeight());
}

TEST_F(NodeRegistrationHandlerTest, testSameUpdateNoop) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");
  auto add_res = registerNode(settings, admin_settings);
  ASSERT_TRUE(add_res.hasValue());

  // Applying an update with the same setting should return UPTODATE.
  NodeRegistrationHandler handler{
      settings, admin_settings, getNodesConfiguration(), store_};
  auto status = handler.updateSelf(add_res.value());
  ASSERT_EQ(Status::UPTODATE, status);
}

}} // namespace facebook::logdevice
