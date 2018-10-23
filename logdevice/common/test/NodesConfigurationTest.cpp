/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"

namespace {

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership::MembershipVersion;
using namespace facebook::logdevice::membership::MaintenanceID;
using RoleSet = NodeServiceDiscovery::RoleSet;

constexpr MaintenanceID::Type DUMMY_MAINTENANCE{2333};

class NodesConfigurationTest : public ::testing::Test {
 public:
  static constexpr NodeServiceDiscovery::RoleSet seq_role{1};
  static constexpr NodeServiceDiscovery::RoleSet storage_role{2};
  static constexpr NodeServiceDiscovery::RoleSet both_role{3};

  static NodeServiceDiscovery genDiscovery(node_index_t n,
                                           RoleSet roles,
                                           std::string location) {
    folly::Optional<NodeLocation> l;
    if (!location.empty()) {
      l = NodeLocation();
      l.value().fromDomainString(location);
    }
    std::string addr = folly::sformat("127.0.0.{}", n);
    return NodeServiceDiscovery{Sockaddr(addr, 4440),
                                Sockaddr(addr, 4441),
                                /*ssl address*/ folly::none,
                                /*admin address*/ folly::none,
                                l,
                                roles,
                                "host" + std::to_string(n)};
  }
};

constexpr NodeServiceDiscovery::RoleSet NodesConfigurationTest::seq_role;
constexpr NodeServiceDiscovery::RoleSet NodesConfigurationTest::storage_role;
constexpr NodeServiceDiscovery::RoleSet NodesConfigurationTest::both_role;

TEST_F(NodesConfigurationTest, EmptyNodesConfigValid) {
  ASSERT_TRUE(NodesConfiguration().validate());
  ASSERT_EQ(EMPTY_VERSION, NodesConfiguration().getVersion());
}

// provision a LD nodes config with:
// 1) four nodes N1, N2, N7, N9
// 2) N1 and N7 have sequencer role; while N1, N2 and N9 have storage role;
// 3) N2 and N9 are metadata storage nodes, metadata logs replicaton is
//    (rack, 2)
std::shared_ptr<const NodesConfiguration> provisionNodes() {
  auto config = std::make_shared<const NodesConfiguration>();
  NodesConfiguration::Update update;

  // 1. provision service discovery config
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  std::map<node_index_t, RoleSet> role_map = {
      {1, NodesConfigurationTest::both_role},
      {2, NodesConfigurationTest::storage_role},
      {7, NodesConfigurationTest::seq_role},
      {9, NodesConfigurationTest::storage_role}};

  for (node_index_t n : NodeSetIndices({1, 2, 7, 9})) {
    update.service_discovery_update->addNode(
        n,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(
                NodesConfigurationTest::genDiscovery(
                    n,
                    role_map[n],
                    n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff"))});
  }
  // 2. provision sequencer config
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.sequencer_config_update->attributes_update =
      std::make_unique<SequencerAttributeConfig::Update>();

  for (node_index_t n : NodeSetIndices({1, 7})) {
    update.sequencer_config_update->membership_update->addNode(
        n,
        {SequencerMembershipTransition::ADD_NODE,
         n == 1 ? 1.0 : 7.0,
         MaintenanceID::MAINTENANCE_PROVISION});

    update.sequencer_config_update->attributes_update->addNode(
        n,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});
  }

  // 3. provision storage config
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();

  for (node_index_t n : NodeSetIndices({1, 2, 9})) {
    update.storage_config_update->membership_update->addShard(
        ShardID(n, 0),
        {n == 1 ? StorageStateTransition::PROVISION_SHARD
                : StorageStateTransition::PROVISION_METADATA_SHARD,
         (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
          Condition::NO_SELF_REPORT_MISSING_DATA |
          Condition::LOCAL_STORE_WRITABLE),
         MaintenanceID::MAINTENANCE_PROVISION});

    update.storage_config_update->attributes_update->addNode(
        n,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ 1,
                                  /*generation*/ 1,
                                  /*exclude_from_nodesets*/ false})});
  }

  // 4. provisoin metadata logs replication
  update.metadata_logs_rep_update =
      std::make_unique<MetaDataLogsReplication::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.metadata_logs_rep_update->replication.assign(
      {{NodeLocationScope::RACK, 2}});

  // 5. fill other update metadata
  update.maintenance = MaintenanceID::MAINTENANCE_PROVISION;
  update.context = "initial provision";

  // finally perform the update
  auto new_config = config->applyUpdate(std::move(update));
  EXPECT_NE(nullptr, new_config);
  return new_config;
}

TEST_F(NodesConfigurationTest, ProvisionBasic) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  // check service discovery info
  auto serv_discovery = config->getServiceDiscovery();
  for (node_index_t n : NodeSetIndices({1, 2, 7, 9})) {
    const NodeServiceDiscovery& node_sd = serv_discovery->nodeAttributesAt(n);
    std::map<node_index_t, RoleSet> role_map = {
        {1, both_role}, {2, storage_role}, {7, seq_role}, {9, storage_role}};

    NodeServiceDiscovery expected = genDiscovery(
        n, role_map[n], n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff");
    EXPECT_EQ(expected, node_sd);
  }

  // check sequencer membership
  auto seq_config = config->getSequencerConfig();
  auto seq_membership = seq_config->getMembership();
  auto seq_attributes = seq_config->getAttributes();
  ASSERT_NE(nullptr, seq_membership);
  ASSERT_NE(nullptr, seq_attributes);

  for (node_index_t n : NodeSetIndices({1, 7})) {
    ASSERT_TRUE(seq_membership->hasNode(n));
    auto result = seq_membership->getNodeState(n);
    EXPECT_TRUE(result.first);
    EXPECT_EQ(
        MaintenanceID::MAINTENANCE_PROVISION, result.second.active_maintenance);
    EXPECT_EQ(n == 1 ? 1.0 : 7.0, result.second.weight);
  }

  // check storage config
  auto storage_config = config->getStorageConfig();
  auto storage_membership = storage_config->getMembership();
  auto storage_attributes = storage_config->getAttributes();
  ASSERT_NE(nullptr, storage_membership);
  ASSERT_NE(nullptr, storage_attributes);

  for (node_index_t n : NodeSetIndices({1, 2, 9})) {
    ASSERT_TRUE(storage_membership->hasNode(n));
    auto result = storage_membership->getShardState(ShardID(n, 0));
    EXPECT_TRUE(result.first);
    EXPECT_EQ(
        MaintenanceID::MAINTENANCE_PROVISION, result.second.active_maintenance);
    // newly provisioned shards should be in rw state
    EXPECT_EQ(StorageState::READ_WRITE, result.second.storage_state);
    EXPECT_EQ(StorageStateFlags::NONE, result.second.flags);
    EXPECT_EQ(MembershipVersion::MIN_VERSION, result.second.since_version);
  }

  // check for metadata nodeset and replication properties
  auto metadata_rep = config->getMetaDataLogsReplication();
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::RACK, 2}}),
            metadata_rep->getReplicationProperty());
  auto meta_shards = storage_membership->getMetaDataShards();
  EXPECT_EQ(std::set<ShardID>({ShardID(2, 0), ShardID(9, 0)}), meta_shards);
  for (node_index_t n : NodeSetIndices({2, 9})) {
    EXPECT_TRUE(meta_shards.count(ShardID(n, 0)) > 0);
    EXPECT_TRUE(storage_membership->canWriteMetaDataToShard(ShardID(n, 0)));
    EXPECT_TRUE(storage_membership->shouldReadMetaDataFromShard(ShardID(n, 0)));
  }
}

TEST_F(NodesConfigurationTest, ChangingServiceDiscoveryAfterProvision) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update;

  // resetting the service discovery for node 2
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();
  update.service_discovery_update->addNode(
      2,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::RESET,
          std::make_unique<NodeServiceDiscovery>(
              NodesConfigurationTest::genDiscovery(
                  2, both_role, "aa.bb.cc.dd.ee"))});
  auto new_config = config->applyUpdate(std::move(update));
  EXPECT_EQ(nullptr, new_config);
  EXPECT_EQ(E::INVALID_PARAM, err);

  // re-provisioning the service discovery for node 9
  NodesConfiguration::Update update2;
  update2.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();
  update2.service_discovery_update->addNode(
      9,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::PROVISION,
          std::make_unique<NodeServiceDiscovery>(
              NodesConfigurationTest::genDiscovery(
                  9, both_role, "aa.bb.cc.dd.ee"))});
  new_config = config->applyUpdate(std::move(update2));
  EXPECT_EQ(nullptr, new_config);
  EXPECT_EQ(E::EXISTS, err);
}

TEST_F(NodesConfigurationTest, RemovingServiceDiscovery) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  {
    NodesConfiguration::Update update;
    // remove service discovery for a node that doesn't exist
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        17,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::NOTINCONFIG, err);
  }
  // remove service discovery for a node still in membership
  for (node_index_t n : NodeSetIndices({1, 7, 9})) {
    NodesConfiguration::Update update;
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        n,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
  // remove N7 from sequencer membership, then it should be OK
  // to remove its service discovery info
  {
    NodesConfiguration::Update update;
    // remove service discovery for a node that doesn't exist
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        7,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});
    update.sequencer_config_update =
        std::make_unique<SequencerConfig::Update>();
    update.sequencer_config_update->membership_update =
        std::make_unique<SequencerMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.sequencer_config_update->membership_update->addNode(
        7,
        {SequencerMembershipTransition::REMOVE_NODE, 0.0, DUMMY_MAINTENANCE});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, new_config);
    EXPECT_FALSE(new_config->getSequencerConfig()->getMembership()->hasNode(7));
  }
}

TEST_F(NodesConfigurationTest, ChangingAttributes) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update;

  // resetting the node attributes for node 2
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();

  auto next_attr = StorageNodeAttribute{/*capacity=*/233.0,
                                        /*num_shards*/ 1,
                                        /*generation*/ 1,
                                        /*exclude_from_nodesets*/ false};

  update.storage_config_update->attributes_update->addNode(
      2,
      {StorageAttributeConfig::UpdateType::RESET,
       std::make_unique<StorageNodeAttribute>(next_attr)});
  auto new_config = config->applyUpdate(std::move(update));
  EXPECT_NE(nullptr, new_config);

  const auto& new_attr =
      new_config->getStorageConfig()->getAttributes()->nodeAttributesAt(2);
  EXPECT_EQ(next_attr, new_attr);
}

TEST_F(NodesConfigurationTest, AddingNodeWithoutServiceDiscoveryOrAttribute) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  {
    NodesConfiguration::Update update;
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.storage_config_update->membership_update->addShard(
        ShardID(17, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
  {
    // with service discovery added but not attributes, membership addition
    // should still fail
    NodesConfiguration::Update update;
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        17,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(
                NodesConfigurationTest::genDiscovery(
                    17, both_role, "aa.bb.cc.dd.ee"))});
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.storage_config_update->membership_update->addShard(
        ShardID(17, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
  {
    // with service discovery and attributes, membership addition
    // should be successful
    NodesConfiguration::Update update;
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        17,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(
                NodesConfigurationTest::genDiscovery(
                    17, both_role, "aa.bb.cc.dd.ee"))});
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->attributes_update =
        std::make_unique<StorageAttributeConfig::Update>();
    update.storage_config_update->attributes_update->addNode(
        17,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ 1,
                                  /*generation*/ 1,
                                  /*exclude_from_nodesets*/ false})});
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.storage_config_update->membership_update->addShard(
        ShardID(17, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, new_config);
    EXPECT_TRUE(new_config->getStorageConfig()->getMembership()->hasNode(17));
  }
}

// adding a node to a membership which does not belong to its provisioned role
// set
TEST_F(NodesConfigurationTest, RoleConflict) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  {
    // node 7 is provisioned to be sequencer-only. try adding it to the storage
    // membership with attribute provided.
    NodesConfiguration::Update update;
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.storage_config_update->attributes_update =
        std::make_unique<StorageAttributeConfig::Update>();
    update.storage_config_update->membership_update->addShard(
        ShardID(7, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    update.storage_config_update->attributes_update->addNode(
        7,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ 1,
                                  /*generation*/ 1,
                                  /*exclude_from_nodesets*/ false})});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
  {
    // node 2 is provisioned to be storage-only. try adding it to the sequencer
    // membership.
    NodesConfiguration::Update update;
    update.sequencer_config_update =
        std::make_unique<SequencerConfig::Update>();
    update.sequencer_config_update->membership_update =
        std::make_unique<SequencerMembership::Update>(
            MembershipVersion::MIN_VERSION);
    update.sequencer_config_update->attributes_update =
        std::make_unique<SequencerAttributeConfig::Update>();

    update.sequencer_config_update->membership_update->addNode(
        2,
        {SequencerMembershipTransition::ADD_NODE,
         1.0,
         MaintenanceID::MAINTENANCE_PROVISION});

    update.sequencer_config_update->attributes_update->addNode(
        2,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});

    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
}

///////////// Legacy Format conversion //////////////

TEST_F(NodesConfigurationTest, LegacyConversion1) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(config, nullptr);
  const auto server_config = config->serverConfig();
  ASSERT_TRUE(NodesConfigLegacyConverter::testWithServerConfig(*server_config));
}

} // namespace
