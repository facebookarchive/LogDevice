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
      flatbuffers::FlatBufferBuilder builder;
      auto config = NodesConfigurationCodecFlatBuffers::serialize(builder, c);
      builder.Finish(config);

      // run flatbuffers internal verification
      auto verifier =
          flatbuffers::Verifier(builder.GetBufferPointer(), builder.GetSize());
      auto res = verifier.VerifyBuffer<
          configuration::nodes::flat_buffer_codec::NodesConfiguration>(nullptr);
      ASSERT_TRUE(res);

      auto config_ptr = flatbuffers::GetRoot<
          configuration::nodes::flat_buffer_codec::NodesConfiguration>(
          builder.GetBufferPointer());
      auto c_deserialized =
          NodesConfigurationCodecFlatBuffers::deserialize(config_ptr);

      ASSERT_NE(nullptr, c_deserialized);
      ASSERT_EQ(c, *c_deserialized);
    }
    {
      // also test serialization with linear buffers
      bool compress = folly::Random::rand64(2) == 0;
      ld_info("Compression: %s", compress ? "enabled" : "disabled");
      std::string str_buf =
          NodesConfigurationCodecFlatBuffers::serialize(c, {compress});
      ASSERT_FALSE(str_buf.empty());
      ld_info("Serialized config blob has %lu bytes", str_buf.size());

      auto version =
          NodesConfigurationCodecFlatBuffers::extractConfigVersion(str_buf);
      ASSERT_TRUE(version.hasValue());
      ASSERT_EQ(c.getVersion(), version.value());

      auto c_deserialized2 = NodesConfigurationCodecFlatBuffers::deserialize(
          (void*)str_buf.data(), str_buf.size());
      ASSERT_NE(nullptr, c_deserialized2);
      ASSERT_EQ(c, *c_deserialized2);
    }
  }
};

TEST_F(NodesConfigurationTest, EmptyNodesConfigValid) {
  // "" as a serialized NC is considered equivalent to a config with an
  // EMPTY_VERSION
  auto config = NodesConfigurationCodecFlatBuffers::deserialize("");
  ASSERT_NE(nullptr, config);
  EXPECT_EQ(MembershipVersion::EMPTY_VERSION, config->getVersion());

  ASSERT_TRUE(NodesConfiguration().validate());
  ASSERT_EQ(EMPTY_VERSION, NodesConfiguration().getVersion());
  checkCodecSerialization(NodesConfiguration());
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
  EXPECT_EQ(std::set<node_index_t>({1, 2, 9}), storage_nodes);

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
  NodesConfiguration::Update update;

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
    checkCodecSerialization(*new_config);
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
  checkCodecSerialization(*new_config);
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

  // check serialization of the converted config
  auto converted_nodes_config =
      NodesConfigLegacyConverter::fromLegacyNodesConfig(
          server_config->getNodesConfig(),
          server_config->getMetaDataLogsConfig(),
          server_config->getVersion());

  ASSERT_NE(converted_nodes_config, nullptr);
  checkCodecSerialization(*converted_nodes_config);
}

TEST_F(NodesConfigurationTest, ExtractVersion) {
  auto version =
      NodesConfigurationCodecFlatBuffers::extractConfigVersion(std::string());
  ASSERT_TRUE(version.hasValue());
  EXPECT_EQ(EMPTY_VERSION, version.value());
  version = NodesConfigurationCodecFlatBuffers::extractConfigVersion(
      std::string("123"));
  ASSERT_FALSE(version.hasValue());
}

} // namespace
