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
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

namespace {

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership::MembershipVersion;
using namespace facebook::logdevice::membership::MaintenanceID;
using namespace facebook::logdevice::NodesConfigurationTestUtil;

using RoleSet = NodeServiceDiscovery::RoleSet;

class NodesConfigurationTest : public ::testing::Test {
 public:
  inline void checkCodecSerialization(const NodesConfiguration& c) {
    VLOG(1) << "config: " << NodesConfigurationCodec::debugJsonString(c);
    {
      auto got = NodesConfigurationThriftConverter::fromThrift(
          NodesConfigurationThriftConverter::toThrift(c));
      ASSERT_NE(nullptr, got);
      ASSERT_EQ(c, *got);
    }
    {
      // also test serialization with linear buffers
      for (auto compress : {false, true}) {
        ld_info("Compression: %s", compress ? "enabled" : "disabled");
        std::string str_buf = NodesConfigurationCodec::serialize(c, {compress});
        ASSERT_FALSE(str_buf.empty());
        ld_info("Serialized config blob has %lu bytes", str_buf.size());

        auto version = NodesConfigurationCodec::extractConfigVersion(str_buf);
        ASSERT_TRUE(version.hasValue());
        ASSERT_EQ(c.getVersion(), version.value());

        auto c_deserialized2 = NodesConfigurationCodec::deserialize(str_buf);
        ASSERT_NE(nullptr, c_deserialized2);
        ASSERT_EQ(c, *c_deserialized2);
      }
    }
  }
};

TEST_F(NodesConfigurationTest, EmptyNodesConfigStringInValid) {
  auto config = NodesConfigurationCodec::deserialize("");
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
    EXPECT_TRUE(result.hasValue());
    EXPECT_EQ(DUMMY_MAINTENANCE, result->active_maintenance);
    EXPECT_EQ(n == 1 ? 1.0 : 7.0, result->getConfiguredWeight());
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
    EXPECT_TRUE(result.hasValue());
    EXPECT_EQ(DUMMY_MAINTENANCE, result->active_maintenance);
    // newly provisioned shards should be in rw state
    EXPECT_EQ(StorageState::READ_WRITE, result->storage_state);

    EXPECT_EQ(StorageStateFlags::NONE, result->flags);
    // Account for the version bump of finalizing the bootstrapping.
    EXPECT_EQ(config->getVersion().val() - 1, result->since_version.val());
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

TEST_F(NodesConfigurationTest, TestMembershipVersionConsistencyValidation) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());

  config = config->applyUpdate(addNewNodeUpdate(*config));
  ASSERT_NE(nullptr, config);

  config = config->applyUpdate(addNewNodeUpdate(*config));
  ASSERT_NE(nullptr, config);

  ASSERT_EQ(6, config->getStorageMembership()->getVersion().val());
  ASSERT_TRUE(config->validate());

  auto c = const_cast<NodesConfiguration*>(config.get());
  c->setVersion(MembershipVersion::Type(2));
  EXPECT_FALSE(c->validate());
}

TEST_F(NodesConfigurationTest, TestGossipDefaultingToDataAddress) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update{};

  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  // Add one node with gossip address
  auto desc1 = genDiscovery(10, both_role, "aa.bb.cc.dd.ee");
  // For the correctness of the test, assert that both addresses are differect.
  ASSERT_NE(desc1.address, desc1.gossip_address.value());

  update.service_discovery_update->addNode(
      10,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::PROVISION,
          std::make_unique<NodeServiceDiscovery>(desc1)});

  // Add one node with gossip address
  auto desc2 = genDiscovery(20, both_role, "aa.bb.cc.dd.ef");
  desc2.gossip_address.reset();
  update.service_discovery_update->addNode(
      20,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::PROVISION,
          std::make_unique<NodeServiceDiscovery>(desc2)});

  auto new_config = config->applyUpdate(update);
  ASSERT_NE(nullptr, new_config);

  // Gossip address is set on N10, Should return the passed gossip address.
  EXPECT_EQ(desc1.gossip_address,
            new_config->getNodeServiceDiscovery(10)->getGossipAddress());

  // Gossip address is not set on N20, should return data address.
  EXPECT_EQ(desc2.address,
            new_config->getNodeServiceDiscovery(20)->getGossipAddress());
}

TEST_F(NodesConfigurationTest, ChangingServiceDiscoveryAfterProvision) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());
  NodesConfiguration::Update update{};

  {
    // Changing the name / addresses of the node should be allowed
    auto new_svc = *config->getNodeServiceDiscovery(2);
    new_svc.name = "NewName";
    new_svc.address = Sockaddr("/tmp/new_addr1");
    new_svc.gossip_address = Sockaddr("/tmp/new_addr2");
    new_svc.ssl_address = Sockaddr("/tmp/new_addr3");

    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        2,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::RESET,
            std::make_unique<NodeServiceDiscovery>(std::move(new_svc))});
    VLOG(1) << "update: " << update.toString();
    auto new_config = config->applyUpdate(std::move(update));
    ASSERT_NE(nullptr, new_config);
    EXPECT_EQ("NewName", new_config->getNodeServiceDiscovery(2)->name);
    EXPECT_EQ(Sockaddr("/tmp/new_addr1"),
              new_config->getNodeServiceDiscovery(2)->address);
    EXPECT_EQ(Sockaddr("/tmp/new_addr2"),
              new_config->getNodeServiceDiscovery(2)->gossip_address);
    EXPECT_EQ(Sockaddr("/tmp/new_addr3"),
              new_config->getNodeServiceDiscovery(2)->ssl_address);
  }

  {
    // resetting the location is not an allowed update
    auto new_svc = *config->getNodeServiceDiscovery(2);
    NodeLocation new_loc;
    new_loc.fromDomainString("aa.bb.cc.dd.zz");
    new_svc.location = new_loc;
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        2,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::RESET,
            std::make_unique<NodeServiceDiscovery>(std::move(new_svc))});
    VLOG(1) << "update: " << update.toString();
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_PARAM, err);
  }

  {
    // resetting the role is not an allowed update
    auto new_svc = *config->getNodeServiceDiscovery(2);
    new_svc.roles[0] = !new_svc.roles[0];
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        2,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::RESET,
            std::make_unique<NodeServiceDiscovery>(std::move(new_svc))});
    VLOG(1) << "update: " << update.toString();
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_PARAM, err);
  }

  {
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
    VLOG(1) << "update2: " << update2.toString();
    auto new_config = config->applyUpdate(std::move(update2));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::EXISTS, err);
  }
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
    VLOG(1) << "update: " << update.toString();
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
    VLOG(1) << "update: " << update.toString();
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
            config->getSequencerMembership()->getVersion());
    update.sequencer_config_update->membership_update->addNode(
        7,
        {SequencerMembershipTransition::REMOVE_NODE,
         false,
         0.0,
         DUMMY_MAINTENANCE});
    VLOG(1) << "update: " << update.toString();
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
  VLOG(1) << "update: " << update.toString();
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
            config->getStorageMembership()->getVersion());
    update.storage_config_update->membership_update->addShard(
        ShardID(17, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    VLOG(1) << "update: " << update.toString();
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
            config->getStorageMembership()->getVersion());
    update.storage_config_update->membership_update->addShard(
        ShardID(17, 0),
        {StorageStateTransition::ADD_EMPTY_SHARD,
         Condition::FORCE,
         DUMMY_MAINTENANCE});
    VLOG(1) << "update: " << update.toString();
    auto new_config = config->applyUpdate(std::move(update));
    EXPECT_EQ(nullptr, new_config);
    EXPECT_EQ(E::INVALID_CONFIG, err);
  }
  {
    // with service discovery and attributes, membership addition
    // should be successful
    NodesConfiguration::Update update = addNewNodeUpdate(*config);
    VLOG(1) << "update: " << update.toString();
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
            config->getStorageMembership()->getVersion());
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
    VLOG(1) << "update: " << update.toString();
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
            config->getSequencerMembership()->getVersion());
    update.sequencer_config_update->attributes_update =
        std::make_unique<SequencerAttributeConfig::Update>();

    update.sequencer_config_update->membership_update->addNode(
        2,
        {SequencerMembershipTransition::ADD_NODE,
         true,
         1.0,
         DUMMY_MAINTENANCE});

    update.sequencer_config_update->attributes_update->addNode(
        2,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});

    VLOG(1) << "update: " << update.toString();
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

TEST_F(NodesConfigurationTest, SingleInvalidAddressIsInvalid) {
  auto svc = std::make_unique<NodeServiceDiscovery>();
  svc->name = "test";
  svc->address = Sockaddr::INVALID;
  svc->gossip_address = Sockaddr("127.0.0.1", 1234);
  svc->roles = 3;

  // Data address is invalid so the whole config is invalid
  EXPECT_FALSE(svc->isValid());

  // Applying an invalid update should not crash
  ServiceDiscoveryConfig::Update update;
  update.addNode(
      node_index_t(0),
      {ServiceDiscoveryConfig::UpdateType::PROVISION, std::move(svc)});

  ServiceDiscoveryConfig new_cfg;
  ServiceDiscoveryConfig cfg;
  EXPECT_EQ(-1, cfg.applyUpdate(update, &new_cfg));
}

TEST_F(NodesConfigurationTest, StorageHash) {
  auto config = provisionNodes();
  ASSERT_TRUE(config->validate());

  const auto original_hash = config->getStorageNodesHash();
  ld_info("Config (version %lu) storage has is 0x%lx.",
          config->getVersion().val_,
          original_hash);

  // change the storage state of an existing shard, verify that node hash
  // changed
  auto nc1 = config->applyUpdate(disablingWriteUpdate(config->getVersion()));
  ASSERT_NE(nullptr, nc1);
  const auto hash1 = nc1->getStorageNodesHash();
  ld_info("Config (version %lu) storage has is 0x%lx.",
          nc1->getVersion().val(),
          hash1);
  ASSERT_NE(original_hash, hash1);

  // change the storage state back, verify that the hash go back to the original
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(nc1->getVersion());

  for (node_index_t n : NodeSetIndices({11, 13})) {
    update.storage_config_update->membership_update->addShard(
        ShardID{n, 0},
        {StorageStateTransition::ABORT_DISABLING_WRITE,
         Condition::FORCE,
         DUMMY_MAINTENANCE,
         /* state_override = */ folly::none});
  }

  auto nc2 = nc1->applyUpdate(std::move(update));
  ASSERT_NE(nullptr, nc2);
  const auto hash2 = nc2->getStorageNodesHash();
  ld_info("Config (version %lu) storage has is 0x%lx.",
          nc2->getVersion().val(),
          hash2);
  ASSERT_EQ(original_hash, hash2);
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
  auto version = NodesConfigurationCodec::extractConfigVersion(std::string());
  ASSERT_FALSE(version.hasValue());
}

TEST_F(NodesConfigurationTest, ExtractVersionError) {
  auto version =
      NodesConfigurationCodec::extractConfigVersion(std::string("123"));
  ASSERT_FALSE(version.hasValue());
}

} // namespace
