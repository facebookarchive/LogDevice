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
#include "logdevice/common/test/MockNodesConfigurationStore.h"

using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;
using testing::_;

namespace facebook::logdevice {
namespace {
std::map<NodeServiceDiscovery::ClientNetworkPriority, std::string>
genUnixAddressesPerNetworkPriority(std::string prefix) {
  return {{NodeServiceDiscovery::ClientNetworkPriority::MEDIUM,
           folly::sformat("{}/medium-priority-address", prefix)},
          {NodeServiceDiscovery::ClientNetworkPriority::LOW,
           folly::sformat("{}/low-priority-address", prefix)}};
}

template <typename M>
decltype(auto) addressesPerNetworkPriorityToString(const M& map) {
  std::map<NodeServiceDiscovery::ClientNetworkPriority, std::string> result;
  for (const auto& [priority, sock_addr] : map) {
    result[priority] = sock_addr.toString();
  }
  return result;
}
} // namespace

class NodeRegistrationHandlerTest : public ::testing::Test {
 public:
  NodeRegistrationHandlerTest() {
    store_ = std::make_shared<InMemNodesConfigurationStore>(
        "/foo", NodesConfigurationCodec::extractConfigVersion);

    // Write an empty NC
    auto ser_nc = NodesConfigurationCodec::serialize(NodesConfiguration{});
    store_->updateConfigSync(
        ser_nc, NodesConfigurationStore::Condition::overwrite());
  }

  ServerSettings buildServerSettings(std::string name) {
    ServerSettings settings = create_default_settings<ServerSettings>();

    settings.name = name;
    settings.unix_socket = folly::sformat("/{}/address", name);
    settings.ssl_unix_socket = folly::sformat("/{}/ssl", name);
    settings.gossip_unix_socket = folly::sformat("/{}/gossip", name);
    settings.server_to_server_unix_socket =
        folly::sformat("/{}/server-to-server", name);
    settings.server_thrift_api_unix_socket =
        folly::sformat("/{}/server-thrift-api", name);
    settings.client_thrift_api_unix_socket =
        folly::sformat("/{}/client-thrift-api", name);
    settings.unix_addresses_per_network_priority =
        genUnixAddressesPerNetworkPriority(
            /*prefix=*/folly::sformat("/{}", name));
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
    auto updateable_nc = std::make_shared<UpdateableNodesConfiguration>();
    updateable_nc->update(getNodesConfiguration());
    NodeRegistrationHandler handler{
        std::move(set), std::move(admin_set), updateable_nc, store_};
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
  EXPECT_EQ("/node1/address", svd->default_client_data_address.toString());
  EXPECT_EQ("/node1/ssl", svd->ssl_address->toString());
  EXPECT_EQ("/node1/gossip", svd->gossip_address->toString());
  EXPECT_EQ("/node1/admin", svd->admin_address->toString());
  EXPECT_EQ(
      "/node1/server-to-server", svd->server_to_server_address->toString());
  EXPECT_EQ(
      "/node1/server-thrift-api", svd->server_thrift_api_address->toString());
  EXPECT_EQ(
      "/node1/client-thrift-api", svd->client_thrift_api_address->toString());
  EXPECT_EQ(genUnixAddressesPerNetworkPriority("/node1"),
            addressesPerNetworkPriorityToString(svd->addresses_per_priority));
  EXPECT_EQ(RoleSet(3), svd->roles);
  EXPECT_EQ("aa.bb.cc.dd.ee", svd->location->toString());

  const auto& storage_attr = nc->getNodeStorageAttribute(index);
  ASSERT_NE(nullptr, storage_attr);
  EXPECT_EQ(2, storage_attr->generation);
  EXPECT_EQ(13, storage_attr->capacity);
  EXPECT_EQ(3, storage_attr->num_shards);

  const auto& shards = nc->getStorageMembership()->getShardStates(index);
  ASSERT_EQ(3, shards.size());
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(0).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(1).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(2).storage_state);

  const auto& seq = nc->getSequencerMembership()->getNodeState(index);
  ASSERT_TRUE(seq.has_value());
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
  settings.server_to_server_unix_socket = "/new/s2s";
  settings.server_thrift_api_unix_socket = "/new/sta";
  settings.client_thrift_api_unix_socket = "/new/cta";
  settings.unix_addresses_per_network_priority =
      genUnixAddressesPerNetworkPriority("/new");
  settings.sequencer_weight = 22;
  settings.storage_capacity = 23;
  settings.admin_enabled = false;

  auto updateable_nc = std::make_shared<UpdateableNodesConfiguration>();
  updateable_nc->update(getNodesConfiguration());
  NodeRegistrationHandler handler{
      settings, admin_settings, updateable_nc, store_};
  auto status = handler.updateSelf(index);
  ASSERT_EQ(Status::OK, status);

  // Get NC
  auto nc = getNodesConfiguration();
  const auto& svd = nc->getNodeServiceDiscovery(index);
  ASSERT_NE(nullptr, svd);
  EXPECT_EQ("node1", svd->name);
  EXPECT_EQ("/new/address", svd->default_client_data_address.toString());
  EXPECT_EQ("/new/ssl", svd->ssl_address->toString());
  EXPECT_EQ("/new/gossip", svd->gossip_address->toString());
  EXPECT_EQ("/new/s2s", svd->server_to_server_address->toString());
  EXPECT_EQ("/new/sta", svd->server_thrift_api_address->toString());
  EXPECT_EQ("/new/cta", svd->client_thrift_api_address->toString());
  EXPECT_EQ(genUnixAddressesPerNetworkPriority("/new"),
            addressesPerNetworkPriorityToString(svd->addresses_per_priority));
  EXPECT_FALSE(svd->admin_address.has_value());
  EXPECT_EQ(RoleSet(3), svd->roles);
  EXPECT_EQ("aa.bb.cc.dd.ee", svd->location->toString());

  const auto& storage_attr = nc->getNodeStorageAttribute(index);
  ASSERT_NE(nullptr, storage_attr);
  EXPECT_EQ(2, storage_attr->generation);
  EXPECT_EQ(23, storage_attr->capacity);
  EXPECT_EQ(3, storage_attr->num_shards);

  const auto& shards = nc->getStorageMembership()->getShardStates(index);
  ASSERT_EQ(3, shards.size());
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(0).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(1).storage_state);
  EXPECT_EQ(StorageState::PROVISIONING, shards.at(2).storage_state);

  const auto& seq = nc->getSequencerMembership()->getNodeState(index);
  ASSERT_TRUE(seq.has_value());
  EXPECT_FALSE(seq->sequencer_enabled);
  EXPECT_EQ(22, seq->getConfiguredWeight());
}

TEST_F(NodeRegistrationHandlerTest, testSameUpdateNoop) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");
  auto add_res = registerNode(settings, admin_settings);
  ASSERT_TRUE(add_res.hasValue());

  // Applying an update with the same setting should return UPTODATE.
  auto updateable_nc = std::make_shared<UpdateableNodesConfiguration>();
  updateable_nc->update(getNodesConfiguration());
  NodeRegistrationHandler handler{
      settings, admin_settings, updateable_nc, store_};
  auto status = handler.updateSelf(add_res.value());
  ASSERT_EQ(Status::UPTODATE, status);
}

// Makes sure that the handler bubbles up unexpected failures.
TEST_F(NodeRegistrationHandlerTest, testRegistrationBubbleUpdateFailures) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");

  auto store = std::make_shared<MockNodesConfigurationStore>();
  EXPECT_CALL(*store, updateConfigSync(_, _, _, _))
      .WillOnce(testing::Return(Status::BADMSG));
  auto updateable_nc = std::make_shared<UpdateableNodesConfiguration>();
  updateable_nc->update(std::make_shared<NodesConfiguration>());
  NodeRegistrationHandler handler{
      settings, admin_settings, std::move(updateable_nc), store};
  auto res = handler.registerSelf(NodeIndicesAllocator{});
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(Status::BADMSG, res.error());
}

// Make sure that NCM VERSION_MISMATCH-es are retried
TEST_F(NodeRegistrationHandlerTest, testRetryOnVersionMismatch) {
  auto settings = buildServerSettings("node1");
  auto admin_settings = buildAdminServerSettings("node1");

  auto store = std::make_shared<MockNodesConfigurationStore>();

  // The expected interactions with the NCS:
  // 1- Initial update fails with VERSION_MISMATCH without providing the new
  // value.
  // 2- The handler, will try to fetch the new version (v2).
  // 3- The new update will also fail with VERSION_MISMATCH but will provide the
  // new value (v3).
  // 4- The update will then succeed.
  testing::InSequence seq;
  EXPECT_CALL(
      *store,
      updateConfigSync(
          _, VersionedConfigStore::Condition(MembershipVersion::Type(0)), _, _))
      .WillOnce(testing::Return(Status::VERSION_MISMATCH));
  EXPECT_CALL(*store, getConfigSync(_, _))
      .WillOnce(testing::Invoke([](auto* value_out, auto) {
        auto nc = NodesConfiguration{}.withVersion(MembershipVersion::Type(1));
        if (value_out) {
          *value_out = NodesConfigurationCodec::serialize(std::move(*nc));
        }
        return Status::OK;
      }));
  EXPECT_CALL(
      *store,
      updateConfigSync(
          _, VersionedConfigStore::Condition(MembershipVersion::Type(1)), _, _))
      .WillOnce(
          testing::Invoke([](auto, auto, auto* version_out, auto* value_out) {
            auto version = MembershipVersion::Type(2);
            auto nc = NodesConfiguration{}.withVersion(version);
            if (version_out) {
              *version_out = version;
            }
            if (value_out) {
              *value_out = NodesConfigurationCodec::serialize(std::move(*nc));
            }
            return Status::VERSION_MISMATCH;
          }));
  EXPECT_CALL(
      *store,
      updateConfigSync(
          _, VersionedConfigStore::Condition(MembershipVersion::Type(2)), _, _))
      .WillOnce(testing::Return(Status::OK));

  auto updateable_nc = std::make_shared<UpdateableNodesConfiguration>();
  updateable_nc->update(std::make_shared<NodesConfiguration>());
  NodeRegistrationHandler handler{
      settings, admin_settings, updateable_nc, store};
  auto res = handler.registerSelf(NodeIndicesAllocator{});
  ASSERT_TRUE(res.hasValue());

  // The updatable should have the the latest value
  ASSERT_EQ(MembershipVersion::Type(2), updateable_nc->get()->getVersion());
}

} // namespace facebook::logdevice
