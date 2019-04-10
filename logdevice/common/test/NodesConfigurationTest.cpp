/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

namespace {

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership::MembershipVersion;
using namespace facebook::logdevice::membership::MaintenanceID;
using RoleSet = NodeServiceDiscovery::RoleSet;

class NodesConfigurationTest : public ::testing::Test {
 public:
  inline void checkCodecSerialization(const NodesConfiguration& c) {
    {
      auto got = NodesConfigurationCodecFlatBuffers::fromThrift(
          NodesConfigurationCodecFlatBuffers::toThrift(c));
      ASSERT_NE(nullptr, got);
      ASSERT_EQ(c, *got);
    }
    {
      // also test serialization with linear buffers
      for (auto compress : {false, true}) {
        ld_info("Compression: %s", compress ? "enabled" : "disabled");
        std::string str_buf =
            NodesConfigurationCodecFlatBuffers::serialize(c, {compress});
        ASSERT_FALSE(str_buf.empty());
        ld_info("Serialized config blob has %lu bytes", str_buf.size());

        auto version =
            NodesConfigurationCodecFlatBuffers::extractConfigVersion(str_buf);
        ASSERT_TRUE(version.hasValue());
        ASSERT_EQ(c.getVersion(), version.value());

        auto c_deserialized2 =
            NodesConfigurationCodecFlatBuffers::deserialize(str_buf);
        ASSERT_NE(nullptr, c_deserialized2);
        ASSERT_EQ(c, *c_deserialized2);
      }
    }
  }
};

TEST_F(NodesConfigurationTest, EmptyNodesConfigStringInValid) {
  auto config = NodesConfigurationCodecFlatBuffers::deserialize("");
  ASSERT_EQ(nullptr, config);
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
    EXPECT_EQ(n == 1 ? 1.0 : 7.0, result.second.getConfiguredWeight());
  }

  // test iterating membership nodes
  std::set<node_index_t> seq_nodes;
  for (node_index_t n : *seq_membership) {
    seq_nodes.insert(n);
  }
  EXPECT_EQ(std::set<node_index_t>({1, 7}), seq_nodes);

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

  // test iterating membership nodes
  std::set<node_index_t> storage_nodes;
  for (node_index_t n : *storage_membership) {
    storage_nodes.insert(n);
  }
  EXPECT_EQ(std::set<node_index_t>({1, 2, 9, 11, 13}), storage_nodes);

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
  checkCodecSerialization(*config);
}

TEST_F(NodesConfigurationTest, ChangingServiceDiscoveryAfterProvision) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update{};

  // resetting the service discovery for node 2
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();
  update.service_discovery_update->addNode(
      2,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::RESET,
          std::make_unique<NodeServiceDiscovery>(
              genDiscovery(2, both_role, "aa.bb.cc.dd.ee"))});
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
              genDiscovery(9, both_role, "aa.bb.cc.dd.ee"))});
  new_config = config->applyUpdate(std::move(update2));
  EXPECT_EQ(nullptr, new_config);
  EXPECT_EQ(E::EXISTS, err);
}

TEST_F(NodesConfigurationTest, RemovingServiceDiscovery) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  {
    NodesConfiguration::Update update{};
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
    NodesConfiguration::Update update{};
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
    NodesConfiguration::Update update{};
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
        {SequencerMembershipTransition::REMOVE_NODE,
         false,
         0.0,
         DUMMY_MAINTENANCE});
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, new_config);
    EXPECT_FALSE(new_config->getSequencerConfig()->getMembership()->hasNode(7));
    checkCodecSerialization(*new_config);
  }
}

TEST_F(NodesConfigurationTest, ChangingAttributes) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update{};

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
  checkCodecSerialization(*new_config);
}

TEST_F(NodesConfigurationTest, AddingNodeWithoutServiceDiscoveryOrAttribute) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  {
    NodesConfiguration::Update update{};
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
    NodesConfiguration::Update update{};
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        17,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(
                genDiscovery(17, both_role, "aa.bb.cc.dd.ee"))});
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
    NodesConfiguration::Update update = addNewNodeUpdate();
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_NE(nullptr, new_config);
    EXPECT_TRUE(new_config->getStorageConfig()->getMembership()->hasNode(17));
    checkCodecSerialization(*new_config);
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
    NodesConfiguration::Update update{};
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
    NodesConfiguration::Update update{};
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
         true,
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

TEST_F(NodesConfigurationTest, touch) {
  SystemTimestamp lb = SystemTimestamp::now();

  auto ptr = provisionNodes();
  const auto& c = *ptr;
  ASSERT_TRUE(c.validate());
  auto v1 = c.getVersion();
  EXPECT_LT(EMPTY_VERSION, v1);
  auto c2 = c.withIncrementedVersionAndTimestamp(
      /* new_nc_version = */ MembershipVersion::Type{67},
      /* new_sequencer_membership_version = */ MembershipVersion::Type{12},
      /* new_storage_membership_version = */ MembershipVersion::Type{67});
  EXPECT_NE(nullptr, c2);
  EXPECT_EQ(c2->getVersion(), MembershipVersion::Type{67});
  EXPECT_EQ(
      MembershipVersion::Type{12}, c2->getSequencerMembership()->getVersion());
  EXPECT_EQ(
      MembershipVersion::Type{67}, c2->getStorageMembership()->getVersion());
  SystemTimestamp ub = SystemTimestamp::now();

  MembershipVersion::Type new_version{123};
  auto prev_ts = c2->getLastChangeTimestamp();
  // We only keep ms granularity
  EXPECT_GE(prev_ts.toMilliseconds(), lb.toMilliseconds());
  EXPECT_GE(ub.toMilliseconds(), prev_ts.toMilliseconds());

  /* sleep override */ std::this_thread::sleep_for(
      std::chrono::milliseconds(2));
  auto c3 = c2->withIncrementedVersionAndTimestamp(new_version);
  EXPECT_NE(nullptr, c3);
  EXPECT_EQ(new_version, c3->getVersion());
  auto now_ts = c3->getLastChangeTimestamp();
  EXPECT_GT(now_ts, prev_ts);

  // if supplied new version is not higher, bump would fail
  auto c4 = c3->withIncrementedVersionAndTimestamp(new_version);
  EXPECT_EQ(nullptr, c4);
  c4 = c3->withIncrementedVersionAndTimestamp(
      /* new_nc_version = */ folly::none,
      /* new_sequencer_membership_version = */
      MembershipVersion::EMPTY_VERSION);
  EXPECT_EQ(nullptr, c4);
  c4 = c3->withIncrementedVersionAndTimestamp(
      /* new_nc_version = */ folly::none,
      /* new_sequencer_membership_version = */ folly::none,
      /* new_storage_membership_version = */
      MembershipVersion::Type{67});
  EXPECT_EQ(nullptr, c4);
}

///////////// Legacy Format conversion //////////////

TEST_F(NodesConfigurationTest, LegacyConversion1) {
  std::shared_ptr<Configuration> config(
      Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf")));
  ASSERT_NE(config, nullptr);
  const auto server_config = config->serverConfig();
  ASSERT_TRUE(NodesConfigLegacyConverter::testWithServerConfig(*server_config));

  // check serialization of the converted config
  auto converted_nodes_config =
      NodesConfigLegacyConverter::fromLegacyNodesConfig(
          server_config->getNodesConfig(),
          server_config->getMetaDataLogsConfig(),
          server_config->getVersion());

  ASSERT_NE(converted_nodes_config, nullptr);
  checkCodecSerialization(*converted_nodes_config);
}

TEST_F(NodesConfigurationTest, ExtractVersionErrorEmptyString) {
  auto version =
      NodesConfigurationCodecFlatBuffers::extractConfigVersion(std::string());
  ASSERT_FALSE(version.hasValue());
}

TEST_F(NodesConfigurationTest, ExtractVersionError) {
  auto version = NodesConfigurationCodecFlatBuffers::extractConfigVersion(
      std::string("123"));
  ASSERT_FALSE(version.hasValue());
}

} // namespace
